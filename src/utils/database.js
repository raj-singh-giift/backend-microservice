import { query, transaction } from '../config/database.js';
import logger from '../config/logger.js';
import { getRequestId } from '../middleware/requestTracker.js';
import debug from 'debug';

const debugDb = debug('app:database:utils');

/**
 * Enhanced logging for database operations
 * @param {string} operation - Database operation name
 * @param {string} queryText - SQL query
 * @param {Array} params - Query parameters
 * @param {number} startTime - Operation start time
 * @param {Object} result - Query result
 * @param {Object} error - Error object if any
 */
const logDatabaseOperation = (operation, queryText, params = [], startTime, result = null, error = null) => {
    const duration = Date.now() - startTime;
    const requestId = getRequestId();

    const logData = {
        operation,
        duration: `${duration}ms`,
        requestId,
        service: 'database-utils',
        affectedRows: result?.rowCount || 0,
        returnedRows: result?.rows?.length || 0
    };

    if (error) {
        logger.error(`Database operation failed: ${operation}`, {
            ...logData,
            error: error.message,
            query: queryText,
            params: params.length > 0 ? `${params.length} parameters` : 'no parameters'
        });
    } else {
        logger.info(`Database operation completed: ${operation}`, logData);
        debugDb(`Query: ${queryText.replace(/\s+/g, ' ').trim()}`);
        debugDb(`Parameters: ${params.length > 0 ? JSON.stringify(params) : 'none'}`);
    }
};

/**
 * Execute query with timeout support
 * @param {string} queryText - SQL query
 * @param {Array} params - Query parameters
 * @param {number} timeout - Timeout in milliseconds
 * @returns {Promise<Object>} Query result
 */
const executeWithTimeout = async (queryText, params, timeout) => {
    return new Promise((resolve, reject) => {
        let timeoutId;
        let isResolved = false;

        // Set up timeout
        if (timeout > 0) {
            timeoutId = setTimeout(() => {
                if (!isResolved) {
                    isResolved = true;
                    reject(new Error(`Query timeout after ${timeout}ms: ${queryText.substring(0, 100)}...`));
                }
            }, timeout);
        }

        // Execute query
        query(queryText, params)
            .then(result => {
                if (!isResolved) {
                    isResolved = true;
                    if (timeoutId) clearTimeout(timeoutId);
                    resolve(result);
                }
            })
            .catch(error => {
                if (!isResolved) {
                    isResolved = true;
                    if (timeoutId) clearTimeout(timeoutId);
                    reject(error);
                }
            });
    });
};

/**
 * Execute transaction with timeout support
 * @param {Function} callback - Transaction callback
 * @param {Object} options - Transaction options
 * @returns {Promise<any>} Transaction result
 */
export const executeTransaction = async (callback, options = {}) => {
    const startTime = Date.now();
    const { timeout = 60000, isolationLevel = null } = options;

    try {
        return await new Promise((resolve, reject) => {
            let timeoutId;
            let isResolved = false;

            // Set up timeout
            if (timeout > 0) {
                timeoutId = setTimeout(() => {
                    if (!isResolved) {
                        isResolved = true;
                        reject(new Error(`Transaction timeout after ${timeout}ms`));
                    }
                }, timeout);
            }

            // Execute transaction
            transaction(async (client) => {
                // Set isolation level if specified
                if (isolationLevel) {
                    await client.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);
                }

                return await callback(client);
            })
                .then(result => {
                    if (!isResolved) {
                        isResolved = true;
                        if (timeoutId) clearTimeout(timeoutId);
                        resolve(result);
                    }
                })
                .catch(error => {
                    if (!isResolved) {
                        isResolved = true;
                        if (timeoutId) clearTimeout(timeoutId);
                        reject(error);
                    }
                });
        });

    } catch (error) {
        const duration = Date.now() - startTime;
        logger.error('Transaction failed:', {
            error: error.message,
            duration: `${duration}ms`,
            service: 'database-utils'
        });
        throw error;
    }
};

/**
 * Bulk insert function with transaction support and timeout
 * @param {string} tableName - Table name
 * @param {Array} dataArray - Array of data objects to insert
 * @param {Object} options - Insert options
 * @returns {Promise<Array>} Array of inserted records
 */
export const bulkInsert = async (tableName, dataArray, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            batchSize = 1000,
            onConflict = null,
            conflictColumns = ['id'],
            returning = '*',
            timeout = 60000
        } = options;

        if (!Array.isArray(dataArray) || dataArray.length === 0) {
            throw new Error('Data array cannot be empty');
        }

        const results = [];

        // Execute with timeout
        await executeTransaction(async (client) => {
            // Process in batches
            for (let i = 0; i < dataArray.length; i += batchSize) {
                const batch = dataArray.slice(i, i + batchSize);

                for (const data of batch) {
                    const result = await insertRecord(tableName, data, {
                        onConflict,
                        conflictColumns,
                        returning,
                        validate: false // Skip individual validation in bulk
                    });
                    if (result) results.push(result);
                }
            }

            return results;
        }, { timeout });

        logDatabaseOperation('bulkInsert', `BULK INSERT INTO ${tableName}`, [`${dataArray.length} records`], startTime, { rowCount: results.length });
        return results;

    } catch (error) {
        logDatabaseOperation('bulkInsert', `BULK INSERT INTO ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Paginated query with timeout support
 * @param {string} baseQuery - Base SQL query without LIMIT/OFFSET
 * @param {Array} params - Query parameters
 * @param {Object} paginationOptions - Pagination options
 * @returns {Promise<Object>} Paginated results
 */
export const paginatedQuery = async (baseQuery, params = [], paginationOptions = {}) => {
    const startTime = Date.now();

    try {
        const {
            page = 1,
            limit = 10,
            orderBy = 'created_at',
            orderDirection = 'DESC',
            includeCount = true,
            maxLimit = 100,
            timeout = 30000
        } = paginationOptions;

        // Validate and sanitize inputs
        const validatedPage = Math.max(1, parseInt(page));
        const validatedLimit = Math.min(Math.max(1, parseInt(limit)), maxLimit);
        const offset = (validatedPage - 1) * validatedLimit;

        // Validate order direction
        const validOrderDirection = ['ASC', 'DESC'].includes(orderDirection.toUpperCase())
            ? orderDirection.toUpperCase()
            : 'DESC';

        let total = 0;

        if (includeCount) {
            // Count total records with optimized query
            const countQuery = `SELECT COUNT(*) as total FROM (${baseQuery}) as count_query`;
            const countResult = timeout > 0
                ? await executeWithTimeout(countQuery, params, timeout)
                : await query(countQuery, params);
            total = parseInt(countResult.rows[0].total);
        }

        // Get paginated data
        const dataQuery = `
            ${baseQuery}
            ORDER BY ${orderBy} ${validOrderDirection}
            LIMIT ${params.length + 1} OFFSET ${params.length + 2}
        `;

        const dataResult = timeout > 0
            ? await executeWithTimeout(dataQuery, [...params, validatedLimit, offset], timeout)
            : await query(dataQuery, [...params, validatedLimit, offset]);

        const result = {
            data: dataResult.rows,
            pagination: {
                page: validatedPage,
                limit: validatedLimit,
                total,
                totalPages: includeCount ? Math.ceil(total / validatedLimit) : null,
                hasNextPage: includeCount ? validatedPage < Math.ceil(total / validatedLimit) : dataResult.rows.length === validatedLimit,
                hasPrevPage: validatedPage > 1,
                offset
            }
        };

        logDatabaseOperation('paginatedQuery', dataQuery, [...params, validatedLimit, offset], startTime, dataResult);
        return result;

    } catch (error) {
        logDatabaseOperation('paginatedQuery', baseQuery, params, startTime, null, error);
        throw error;
    }
};

/**
 * Pattern matching queries with timeout
 * @param {string} tableName - Table name
 * @param {Object} patterns - Pattern matching criteria
 * @param {Object} options - Query options
 * @returns {Promise<Array>} Matching records
 */
export const patternMatch = async (tableName, patterns, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            select = '*',
            orderBy = null,
            limit = null,
            caseSensitive = false,
            timeout = 30000
        } = options;

        let whereClauses = [];
        let params = [];
        let paramIndex = 1;

        Object.entries(patterns).forEach(([field, pattern]) => {
            const { type, value, flags = '' } = pattern;

            switch (type) {
                case 'like':
                    const operator = caseSensitive ? 'LIKE' : 'ILIKE';
                    whereClauses.push(`${field} ${operator} ${paramIndex++}`);
                    params.push(value);
                    break;

                case 'regex':
                    const regexOp = caseSensitive ? '~' : '~*';
                    whereClauses.push(`${field} ${regexOp} ${paramIndex++}`);
                    params.push(value);
                    break;

                case 'startsWith':
                    const startsOp = caseSensitive ? 'LIKE' : 'ILIKE';
                    whereClauses.push(`${field} ${startsOp} ${paramIndex++}`);
                    params.push(`${value}%`);
                    break;

                case 'endsWith':
                    const endsOp = caseSensitive ? 'LIKE' : 'ILIKE';
                    whereClauses.push(`${field} ${endsOp} ${paramIndex++}`);
                    params.push(`%${value}`);
                    break;

                case 'contains':
                    const containsOp = caseSensitive ? 'LIKE' : 'ILIKE';
                    whereClauses.push(`${field} ${containsOp} ${paramIndex++}`);
                    params.push(`%${value}%`);
                    break;

                case 'fulltext':
                    whereClauses.push(`to_tsvector('english', ${field}) @@ plainto_tsquery('english', ${paramIndex++})`);
                    params.push(value);
                    break;

                default:
                    throw new Error(`Unsupported pattern type: ${type}`);
            }
        });

        const queryText = `
            SELECT ${select}
            FROM ${tableName}
            WHERE ${whereClauses.join(' AND ')}
            ${orderBy ? `ORDER BY ${orderBy}` : ''}
            ${limit ? `LIMIT ${limit}` : ''}
        `;

        const result = timeout > 0
            ? await executeWithTimeout(queryText, params, timeout)
            : await query(queryText, params);

        logDatabaseOperation('patternMatch', queryText, params, startTime, result);
        return result.rows;

    } catch (error) {
        logDatabaseOperation('patternMatch', `PATTERN MATCH ON ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Generic insert function with enhanced conflict handling
 * @param {string} tableName - Table name
 * @param {Object} data - Data to insert
 * @param {Object} options - Insert options
 * @returns {Promise<Object>} Insert result
 */
export const insertRecord = async (tableName, data, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            onConflict = null,
            conflictColumns = ['id'],
            returning = '*',
            validate = true
        } = options;

        if (validate && (!data || Object.keys(data).length === 0)) {
            throw new Error('Insert data cannot be empty');
        }

        const columns = Object.keys(data);
        const values = Object.values(data);
        const placeholders = values.map((_, index) => `$${index + 1}`);

        let queryText = `
            INSERT INTO ${tableName} (${columns.join(', ')})
            VALUES (${placeholders.join(', ')})
        `;

        if (onConflict === 'ignore') {
            queryText += ` ON CONFLICT (${conflictColumns.join(', ')}) DO NOTHING`;
        } else if (onConflict === 'update') {
            const updateSet = columns
                .filter(col => !conflictColumns.includes(col))
                .map(col => `${col} = EXCLUDED.${col}`)
                .join(', ');

            if (updateSet) {
                queryText += ` ON CONFLICT (${conflictColumns.join(', ')}) DO UPDATE SET ${updateSet}`;
            }
        }

        queryText += ` RETURNING ${returning}`;

        const result = await query(queryText, values);

        logDatabaseOperation('insertRecord', queryText, values, startTime, result);
        return result.rows[0];

    } catch (error) {
        logDatabaseOperation('insertRecord', `INSERT INTO ${tableName}`, Object.values(data), startTime, null, error);
        throw error;
    }
};

/**
 * Enhanced update function with optimistic locking
 * @param {string} tableName - Table name
 * @param {Object} data - Data to update
 * @param {Object} whereConditions - Where conditions
 * @param {Object} options - Update options
 * @returns {Promise<Object>} Update result
 */
export const updateRecord = async (tableName, data, whereConditions, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            returning = '*',
            optimisticLocking = false,
            versionColumn = 'version'
        } = options;

        if (!data || Object.keys(data).length === 0) {
            throw new Error('Update data cannot be empty');
        }

        const dataColumns = Object.keys(data);
        const dataValues = Object.values(data);
        const whereColumns = Object.keys(whereConditions);
        const whereValues = Object.values(whereConditions);

        // Add optimistic locking if enabled
        if (optimisticLocking && data[versionColumn] !== undefined) {
            whereConditions[versionColumn] = data[versionColumn];
            data[versionColumn] = data[versionColumn] + 1;
        }

        const setClause = dataColumns.map((col, index) => `${col} = $${index + 1}`).join(', ');
        const whereClause = whereColumns.map((col, index) => `${col} = $${dataValues.length + index + 1}`).join(' AND ');

        const queryText = `
            UPDATE ${tableName}
            SET ${setClause}, updated_at = NOW()
            WHERE ${whereClause}
            RETURNING ${returning}
        `;

        const result = await query(queryText, [...dataValues, ...whereValues]);

        if (result.rows.length === 0) {
            throw new Error(`No records updated in ${tableName}. Record may not exist or version conflict occurred.`);
        }

        logDatabaseOperation('updateRecord', queryText, [...dataValues, ...whereValues], startTime, result);
        return result.rows[0];

    } catch (error) {
        logDatabaseOperation('updateRecord', `UPDATE ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Upsert function (insert or update)
 * @param {string} tableName - Table name
 * @param {Object} data - Data to upsert
 * @param {Array} conflictColumns - Columns to check for conflict
 * @param {Object} options - Upsert options
 * @returns {Promise<Object>} Upsert result
 */
export const upsertRecord = async (tableName, data, conflictColumns, options = {}) => {
    const startTime = Date.now();

    try {
        const { returning = '*', excludeFromUpdate = [] } = options;

        const columns = Object.keys(data);
        const values = Object.values(data);
        const placeholders = values.map((_, index) => `$${index + 1}`);

        const updateColumns = columns.filter(col =>
            !conflictColumns.includes(col) && !excludeFromUpdate.includes(col)
        );

        const updateSet = updateColumns.map(col => `${col} = EXCLUDED.${col}`).join(', ');

        const queryText = `
            INSERT INTO ${tableName} (${columns.join(', ')})
            VALUES (${placeholders.join(', ')})
            ON CONFLICT (${conflictColumns.join(', ')}) 
            DO UPDATE SET ${updateSet}, updated_at = NOW()
            RETURNING ${returning}
        `;

        const result = await query(queryText, values);

        logDatabaseOperation('upsertRecord', queryText, values, startTime, result);
        return result.rows[0];

    } catch (error) {
        logDatabaseOperation('upsertRecord', `UPSERT ${tableName}`, Object.values(data), startTime, null, error);
        throw error;
    }
};

/**
 * Enhanced delete function with cascade options
 * @param {string} tableName - Table name
 * @param {Object} whereConditions - Where conditions
 * @param {Object} options - Delete options
 * @returns {Promise<Object>} Delete result
 */
export const deleteRecord = async (tableName, whereConditions, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            returning = 'id',
            softDelete = false,
            cascadeDelete = [],
            force = false
        } = options;

        const whereColumns = Object.keys(whereConditions);
        const whereValues = Object.values(whereConditions);

        if (whereColumns.length === 0 && !force) {
            throw new Error('Delete operation requires WHERE conditions or force=true');
        }

        const whereClause = whereColumns.length > 0
            ? whereColumns.map((col, index) => `${col} = $${index + 1}`).join(' AND ')
            : '1=1';

        let queryText;

        if (softDelete) {
            queryText = `
                UPDATE ${tableName}
                SET deleted_at = NOW(), updated_at = NOW()
                WHERE ${whereClause} AND deleted_at IS NULL
                RETURNING ${returning}
            `;
        } else {
            queryText = `
                DELETE FROM ${tableName}
                WHERE ${whereClause}
                RETURNING ${returning}
            `;
        }

        const result = await query(queryText, whereValues);

        // Handle cascade deletes if specified
        if (cascadeDelete.length > 0 && result.rows.length > 0) {
            for (const cascade of cascadeDelete) {
                const { table, foreignKey } = cascade;
                const deletedIds = result.rows.map(row => row.id);

                await deleteRecord(table, { [foreignKey]: deletedIds }, {
                    softDelete,
                    force: true
                });
            }
        }

        logDatabaseOperation('deleteRecord', queryText, whereValues, startTime, result);
        return result.rows[0];

    } catch (error) {
        logDatabaseOperation('deleteRecord', `DELETE FROM ${tableName}`, Object.values(whereConditions), startTime, null, error);
        throw error;
    }
};

/**
 * Find record by ID with caching support
 * @param {string} tableName - Table name
 * @param {string|number} id - Record ID
 * @param {Object} options - Find options
 * @returns {Promise<Object|null>} Found record or null
 */
export const findById = async (tableName, id, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            columns = '*',
            includeSoftDeleted = false
        } = options;

        let whereClause = 'id = $1';
        if (!includeSoftDeleted) {
            whereClause += ' AND (deleted_at IS NULL OR deleted_at > NOW())';
        }

        const queryText = `
            SELECT ${columns}
            FROM ${tableName}
            WHERE ${whereClause}
        `;

        const result = await query(queryText, [id]);

        logDatabaseOperation('findById', queryText, [id], startTime, result);
        return result.rows[0] || null;

    } catch (error) {
        logDatabaseOperation('findById', `SELECT FROM ${tableName}`, [id], startTime, null, error);
        throw error;
    }
};

/**
 * Find records with flexible conditions
 * @param {string} tableName - Table name
 * @param {Object} conditions - Search conditions
 * @param {Object} options - Find options
 * @returns {Promise<Array>} Found records
 */
export const findWhere = async (tableName, conditions = {}, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            columns = '*',
            orderBy = 'created_at',
            orderDirection = 'DESC',
            limit = null,
            includeSoftDeleted = false
        } = options;

        const whereColumns = Object.keys(conditions);
        const whereValues = Object.values(conditions);

        let whereClause = whereColumns.length > 0
            ? whereColumns.map((col, index) => `${col} = $${index + 1}`).join(' AND ')
            : '1=1';

        if (!includeSoftDeleted) {
            whereClause += whereColumns.length > 0
                ? ' AND (deleted_at IS NULL OR deleted_at > NOW())'
                : ' WHERE (deleted_at IS NULL OR deleted_at > NOW())';
        }

        let queryText = `
            SELECT ${columns}
            FROM ${tableName}
            WHERE ${whereClause}
            ORDER BY ${orderBy} ${orderDirection}
        `;

        if (limit) {
            queryText += ` LIMIT ${parseInt(limit)}`;
        }

        const result = await query(queryText, whereValues);

        logDatabaseOperation('findWhere', queryText, whereValues, startTime, result);
        return result.rows;

    } catch (error) {
        logDatabaseOperation('findWhere', `SELECT FROM ${tableName}`, Object.values(conditions), startTime, null, error);
        throw error;
    }
};

/**
 * Count records with conditions
 * @param {string} tableName - Table name
 * @param {Object} conditions - Count conditions
 * @param {Object} options - Count options
 * @returns {Promise<number>} Record count
 */
export const countRecords = async (tableName, conditions = {}, options = {}) => {
    const startTime = Date.now();

    try {
        const { includeSoftDeleted = false } = options;

        const whereColumns = Object.keys(conditions);
        const whereValues = Object.values(conditions);

        let whereClause = whereColumns.length > 0
            ? whereColumns.map((col, index) => `${col} = $${index + 1}`).join(' AND ')
            : '1=1';

        if (!includeSoftDeleted) {
            whereClause += whereColumns.length > 0
                ? ' AND (deleted_at IS NULL OR deleted_at > NOW())'
                : ' WHERE (deleted_at IS NULL OR deleted_at > NOW())';
        }

        const queryText = `
            SELECT COUNT(*) as count
            FROM ${tableName}
            WHERE ${whereClause}
        `;

        const result = await query(queryText, whereValues);
        const count = parseInt(result.rows[0].count);

        logDatabaseOperation('countRecords', queryText, whereValues, startTime, result);
        return count;

    } catch (error) {
        logDatabaseOperation('countRecords', `COUNT FROM ${tableName}`, Object.values(conditions), startTime, null, error);
        throw error;
    }
};

/**
 * Execute raw query with proper logging and timeout
 * @param {string} queryText - SQL query
 * @param {Array} params - Query parameters
 * @param {Object} options - Query options
 * @returns {Promise<Object>} Query result
 */
export const executeRawQuery = async (queryText, params = [], options = {}) => {
    const startTime = Date.now();

    try {
        const { expectSingleRow = false, operation = 'rawQuery', timeout = 30000 } = options;

        // Execute with timeout
        const result = timeout > 0
            ? await executeWithTimeout(queryText, params, timeout)
            : await query(queryText, params);

        logDatabaseOperation(operation, queryText, params, startTime, result);

        if (expectSingleRow) {
            return result.rows[0] || null;
        }

        return result.rows;

    } catch (error) {
        logDatabaseOperation(options.operation || 'rawQuery', queryText, params, startTime, null, error);
        throw error;
    }
};

/**
 * Enhanced stored procedure execution with timeout
 * @param {string} procedureName - Stored procedure name
 * @param {Array} params - Procedure parameters
 * @param {Object} options - Execution options
 * @returns {Promise<Object>} Procedure result
 */
export const executeStoredProcedure = async (procedureName, params = [], options = {}) => {
    const startTime = Date.now();

    try {
        const { timeout = 30000 } = options;

        const placeholders = params.map((_, index) => `${index + 1}`).join(', ');
        const queryText = `CALL ${procedureName}(${placeholders})`;

        // Execute with timeout
        const result = timeout > 0
            ? await executeWithTimeout(queryText, params, timeout)
            : await query(queryText, params);

        logDatabaseOperation('executeStoredProcedure', queryText, params, startTime, result);
        return result;

    } catch (error) {
        logDatabaseOperation('executeStoredProcedure', `CALL ${procedureName}`, params, startTime, null, error);
        throw error;
    }
};

/**
 * Enhanced function execution with timeout
 * @param {string} functionName - Function name
 * @param {Array} params - Function parameters
 * @param {Object} options - Execution options
 * @returns {Promise<Object>} Function result
 */
export const executeFunction = async (functionName, params = [], options = {}) => {
    const startTime = Date.now();

    try {
        const { expectSingleRow = false, timeout = 30000 } = options;

        const placeholders = params.map((_, index) => `${index + 1}`).join(', ');
        const queryText = `SELECT * FROM ${functionName}(${placeholders})`;

        // Execute with timeout
        const result = timeout > 0
            ? await executeWithTimeout(queryText, params, timeout)
            : await query(queryText, params);

        logDatabaseOperation('executeFunction', queryText, params, startTime, result);

        if (expectSingleRow) {
            return result.rows[0] || null;
        }

        return result.rows;

    } catch (error) {
        logDatabaseOperation('executeFunction', `SELECT FROM ${functionName}`, params, startTime, null, error);
        throw error;
    }
};

/**
 * Database health check utility
 * @returns {Promise<Object>} Health check result
 */
export const databaseHealthCheck = async () => {
    const startTime = Date.now();

    try {
        const result = await query('SELECT NOW() as server_time, version() as server_version');
        const duration = Date.now() - startTime;

        logger.info('Database health check passed', {
            duration: `${duration}ms`,
            service: 'database-utils'
        });

        return {
            status: 'healthy',
            serverTime: result.rows[0].server_time,
            serverVersion: result.rows[0].server_version,
            responseTime: duration
        };

    } catch (error) {
        logger.error('Database health check failed', {
            error: error.message,
            service: 'database-utils'
        });

        return {
            status: 'unhealthy',
            error: error.message,
            responseTime: Date.now() - startTime
        };
    }
};

/**
 * Advanced query builder with support for complex operations
 * @param {string} tableName - Table name
 * @param {Object} options - Query options
 * @returns {Promise<Array>} Query results
 */
export const advancedQuery = async (tableName, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            select = '*',
            where = {},
            whereRaw = null,
            joins = [],
            groupBy = null,
            having = null,
            orderBy = null,
            limit = null,
            offset = null,
            distinct = false,
            includeSoftDeleted = false
        } = options;

        let queryParts = [];
        let params = [];
        let paramIndex = 1;

        // SELECT clause
        const selectClause = distinct ? `DISTINCT ${select}` : select;
        queryParts.push(`SELECT ${selectClause}`);

        // FROM clause
        queryParts.push(`FROM ${tableName}`);

        // JOIN clauses
        if (joins.length > 0) {
            joins.forEach(join => {
                const { type = 'INNER', table, on } = join;
                queryParts.push(`${type} JOIN ${table} ON ${on}`);
            });
        }

        // WHERE clause
        let whereClauses = [];

        // Standard where conditions
        Object.entries(where).forEach(([key, value]) => {
            if (Array.isArray(value)) {
                const placeholders = value.map(() => `${paramIndex++}`).join(', ');
                whereClauses.push(`${key} IN (${placeholders})`);
                params.push(...value);
            } else if (value === null) {
                whereClauses.push(`${key} IS NULL`);
            } else {
                whereClauses.push(`${key} = ${paramIndex++}`);
                params.push(value);
            }
        });

        // Raw where clause
        if (whereRaw) {
            whereClauses.push(whereRaw);
        }

        // Soft delete filter
        if (!includeSoftDeleted) {
            whereClauses.push('(deleted_at IS NULL OR deleted_at > NOW())');
        }

        if (whereClauses.length > 0) {
            queryParts.push(`WHERE ${whereClauses.join(' AND ')}`);
        }

        // GROUP BY clause
        if (groupBy) {
            queryParts.push(`GROUP BY ${groupBy}`);
        }

        // HAVING clause
        if (having) {
            queryParts.push(`HAVING ${having}`);
        }

        // ORDER BY clause
        if (orderBy) {
            queryParts.push(`ORDER BY ${orderBy}`);
        }

        // LIMIT clause
        if (limit) {
            queryParts.push(`LIMIT ${parseInt(limit)}`);
        }

        // OFFSET clause
        if (offset) {
            queryParts.push(`OFFSET ${parseInt(offset)}`);
        }

        const queryText = queryParts.join(' ');
        const result = await query(queryText, params);

        logDatabaseOperation('advancedQuery', queryText, params, startTime, result);
        return result.rows;

    } catch (error) {
        logDatabaseOperation('advancedQuery', `ADVANCED QUERY ON ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Group by query with aggregations
 * @param {string} tableName - Table name
 * @param {Object} options - Group by options
 * @returns {Promise<Array>} Grouped results
 */
export const groupByQuery = async (tableName, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            groupBy,
            select = [],
            aggregates = {},
            where = {},
            having = null,
            orderBy = null,
            limit = null
        } = options;

        if (!groupBy) {
            throw new Error('groupBy field is required');
        }

        // Build select clause with aggregates
        let selectParts = [groupBy];

        // Add custom select fields
        if (select.length > 0) {
            selectParts.push(...select);
        }

        // Add aggregate functions
        Object.entries(aggregates).forEach(([alias, aggFunc]) => {
            selectParts.push(`${aggFunc} AS ${alias}`);
        });

        const result = await advancedQuery(tableName, {
            select: selectParts.join(', '),
            where,
            groupBy,
            having,
            orderBy,
            limit
        });

        logDatabaseOperation('groupByQuery', `GROUP BY ${groupBy} ON ${tableName}`, [], startTime, { rows: result });
        return result;

    } catch (error) {
        logDatabaseOperation('groupByQuery', `GROUP BY ON ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Get top N records with various criteria
 * @param {string} tableName - Table name
 * @param {Object} options - Top query options
 * @returns {Promise<Array>} Top records
 */
export const getTopRecords = async (tableName, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            limit = 10,
            orderBy = 'created_at DESC',
            where = {},
            groupBy = null,
            having = null,
            withTies = false
        } = options;

        let queryOptions = {
            where,
            orderBy,
            limit: withTies ? null : limit
        };

        if (groupBy) {
            queryOptions.groupBy = groupBy;
        }

        if (having) {
            queryOptions.having = having;
        }

        // Handle WITH TIES using window functions
        if (withTies) {
            const [orderColumn, direction = 'DESC'] = orderBy.split(' ');
            queryOptions.select = `*, RANK() OVER (ORDER BY ${orderColumn} ${direction}) as rank_num`;
            queryOptions.orderBy = orderBy;
            queryOptions.limit = null;
        }

        let result = await advancedQuery(tableName, queryOptions);

        // Filter by rank if using WITH TIES
        if (withTies) {
            result = result.filter(row => row.rank_num <= limit);
        }

        logDatabaseOperation('getTopRecords', `TOP ${limit} FROM ${tableName}`, [], startTime, { rows: result });
        return result;

    } catch (error) {
        logDatabaseOperation('getTopRecords', `TOP RECORDS FROM ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Window functions query
 * @param {string} tableName - Table name
 * @param {Object} options - Window function options
 * @returns {Promise<Array>} Results with window calculations
 */
export const windowQuery = async (tableName, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            select = '*',
            windowFunctions = [],
            where = {},
            orderBy = null,
            limit = null
        } = options;

        let selectParts = [select];

        // Add window functions
        windowFunctions.forEach(wf => {
            const {
                func,
                column = '',
                alias,
                partitionBy = null,
                orderBy: windowOrder = null,
                frameClause = null
            } = wf;

            let windowClause = '';

            if (partitionBy || windowOrder || frameClause) {
                windowClause = ' OVER (';

                if (partitionBy) {
                    windowClause += `PARTITION BY ${partitionBy}`;
                }

                if (windowOrder) {
                    windowClause += `${partitionBy ? ' ' : ''}ORDER BY ${windowOrder}`;
                }

                if (frameClause) {
                    windowClause += ` ${frameClause}`;
                }

                windowClause += ')';
            }

            const funcCall = column ? `${func}(${column})` : `${func}()`;
            selectParts.push(`${funcCall}${windowClause} AS ${alias}`);
        });

        const result = await advancedQuery(tableName, {
            select: selectParts.join(', '),
            where,
            orderBy,
            limit
        });

        logDatabaseOperation('windowQuery', `WINDOW FUNCTIONS ON ${tableName}`, [], startTime, { rows: result });
        return result;

    } catch (error) {
        logDatabaseOperation('windowQuery', `WINDOW QUERY ON ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Aggregation query with multiple functions
 * @param {string} tableName - Table name
 * @param {Object} options - Aggregation options
 * @returns {Promise<Object>} Aggregation results
 */
export const aggregateQuery = async (tableName, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            aggregates = {},
            where = {},
            groupBy = null,
            having = null
        } = options;

        if (Object.keys(aggregates).length === 0) {
            throw new Error('At least one aggregate function is required');
        }

        // Build aggregate functions
        const aggregateParts = Object.entries(aggregates).map(([alias, config]) => {
            if (typeof config === 'string') {
                return `${config} AS ${alias}`;
            }

            const { func, column, distinct = false, filter = null } = config;
            let aggFunc = `${func}(${distinct ? 'DISTINCT ' : ''}${column || '*'})`;

            if (filter) {
                aggFunc += ` FILTER (WHERE ${filter})`;
            }

            return `${aggFunc} AS ${alias}`;
        });

        const result = await advancedQuery(tableName, {
            select: aggregateParts.join(', '),
            where,
            groupBy,
            having
        });

        logDatabaseOperation('aggregateQuery', `AGGREGATES ON ${tableName}`, [], startTime, { rows: result });
        return groupBy ? result : result[0];

    } catch (error) {
        logDatabaseOperation('aggregateQuery', `AGGREGATION ON ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Range queries (date ranges, numeric ranges, etc.)
 * @param {string} tableName - Table name
 * @param {Object} ranges - Range criteria
 * @param {Object} options - Query options
 * @returns {Promise<Array>} Records within ranges
 */
export const rangeQuery = async (tableName, ranges, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            select = '*',
            orderBy = null,
            limit = null,
            includeBounds = [true, true] // [includeLower, includeUpper]
        } = options;

        let whereClauses = [];
        let params = [];
        let paramIndex = 1;

        Object.entries(ranges).forEach(([field, range]) => {
            const { min, max, type = 'numeric' } = range;
            const [includeLower, includeUpper] = includeBounds;

            if (min !== undefined && min !== null) {
                const operator = includeLower ? '>=' : '>';
                whereClauses.push(`${field} ${operator} ${paramIndex++}`);
                params.push(min);
            }

            if (max !== undefined && max !== null) {
                const operator = includeUpper ? '<=' : '<';
                whereClauses.push(`${field} ${operator} ${paramIndex++}`);
                params.push(max);
            }

            // Special handling for date ranges
            if (type === 'date' && range.interval) {
                whereClauses.push(`${field} >= NOW() - INTERVAL '${range.interval}'`);
            }
        });

        const queryText = `
            SELECT ${select}
            FROM ${tableName}
            WHERE ${whereClauses.join(' AND ')}
            ${orderBy ? `ORDER BY ${orderBy}` : ''}
            ${limit ? `LIMIT ${limit}` : ''}
        `;

        const result = await query(queryText, params);

        logDatabaseOperation('rangeQuery', queryText, params, startTime, result);
        return result.rows;

    } catch (error) {
        logDatabaseOperation('rangeQuery', `RANGE QUERY ON ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * JSON operations query (for JSONB columns)
 * @param {string} tableName - Table name
 * @param {Object} jsonOps - JSON operations
 * @param {Object} options - Query options
 * @returns {Promise<Array>} Query results
 */
export const jsonQuery = async (tableName, jsonOps, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            select = '*',
            orderBy = null,
            limit = null
        } = options;

        let whereClauses = [];
        let params = [];
        let paramIndex = 1;

        Object.entries(jsonOps).forEach(([field, operations]) => {
            operations.forEach(op => {
                const { type, path, value, operator = '=' } = op;

                switch (type) {
                    case 'contains':
                        whereClauses.push(`${field} @> ${paramIndex++}`);
                        params.push(JSON.stringify(value));
                        break;

                    case 'containedBy':
                        whereClauses.push(`${field} <@ ${paramIndex++}`);
                        params.push(JSON.stringify(value));
                        break;

                    case 'hasKey':
                        whereClauses.push(`${field} ? ${paramIndex++}`);
                        params.push(path);
                        break;

                    case 'hasKeys':
                        whereClauses.push(`${field} ?& ${paramIndex++}`);
                        params.push(value);
                        break;

                    case 'hasAnyKey':
                        whereClauses.push(`${field} ?| ${paramIndex++}`);
                        params.push(value);
                        break;

                    case 'pathExists':
                        whereClauses.push(`${field} #> ${paramIndex++} IS NOT NULL`);
                        params.push(`{${path}}`);
                        break;

                    case 'pathValue':
                        whereClauses.push(`${field} #>> ${paramIndex++} ${operator} ${paramIndex++}`);
                        params.push(`{${path}}`, value);
                        break;

                    default:
                        throw new Error(`Unsupported JSON operation: ${type}`);
                }
            });
        });

        const queryText = `
            SELECT ${select}
            FROM ${tableName}
            WHERE ${whereClauses.join(' AND ')}
            ${orderBy ? `ORDER BY ${orderBy}` : ''}
            ${limit ? `LIMIT ${limit}` : ''}
        `;

        const result = await query(queryText, params);

        logDatabaseOperation('jsonQuery', queryText, params, startTime, result);
        return result.rows;

    } catch (error) {
        logDatabaseOperation('jsonQuery', `JSON QUERY ON ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Exists/Not Exists subquery
 * @param {string} tableName - Main table name
 * @param {Object} subquery - Subquery configuration
 * @param {Object} options - Query options
 * @returns {Promise<Array>} Query results
 */
export const existsQuery = async (tableName, subquery, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            select = '*',
            where = {},
            orderBy = null,
            limit = null,
            exists = true
        } = options;

        const {
            table: subTable,
            where: subWhere,
            correlation
        } = subquery;

        let params = [];
        let paramIndex = 1;

        // Build main where clauses
        let mainWhereClauses = [];
        Object.entries(where).forEach(([key, value]) => {
            mainWhereClauses.push(`${key} = ${paramIndex++}`);
            params.push(value);
        });

        // Build subquery where clauses
        let subWhereClauses = [];
        Object.entries(subWhere).forEach(([key, value]) => {
            subWhereClauses.push(`${key} = ${paramIndex++}`);
            params.push(value);
        });

        // Add correlation condition
        if (correlation) {
            subWhereClauses.push(correlation);
        }

        const existsOperator = exists ? 'EXISTS' : 'NOT EXISTS';
        const subQueryText = `
            SELECT 1 FROM ${subTable}
            WHERE ${subWhereClauses.join(' AND ')}
        `;

        mainWhereClauses.push(`${existsOperator} (${subQueryText})`);

        const queryText = `
            SELECT ${select}
            FROM ${tableName}
            WHERE ${mainWhereClauses.join(' AND ')}
            ${orderBy ? `ORDER BY ${orderBy}` : ''}
            ${limit ? `LIMIT ${limit}` : ''}
        `;

        const result = await query(queryText, params);

        logDatabaseOperation('existsQuery', queryText, params, startTime, result);
        return result.rows;

    } catch (error) {
        logDatabaseOperation('existsQuery', `EXISTS QUERY ON ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Get table statistics
 * @param {string} tableName - Table name
 * @returns {Promise<Object>} Table statistics
 */
export const getTableStats = async (tableName) => {
    const startTime = Date.now();

    try {
        const queryText = `
            SELECT 
                schemaname,
                tablename,
                attname,
                n_distinct,
                most_common_vals,
                most_common_freqs,
                histogram_bounds
            FROM pg_stats 
            WHERE tablename = $1
        `;

        const result = await query(queryText, [tableName]);

        logDatabaseOperation('getTableStats', queryText, [tableName], startTime, result);
        return result.rows;

    } catch (error) {
        logDatabaseOperation('getTableStats', `STATS FOR ${tableName}`, [tableName], startTime, null, error);
        throw error;
    }
};