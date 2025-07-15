import { query, transaction } from '../config/database.js';
import logger from '../config/logger.js';
import { getRequestId } from '../middleware/requestTracker.js';
import { cacheService } from '../services/cacheService.js';
import debug from 'debug';

const debugDb = debug('app:database:utils');

// Cache for table schema information
const schemaCache = new Map();

/**
 * Enhanced logging for database operations
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
 * Get table schema information with caching
 * @param {string} tableName - Table name
 * @returns {Promise<Object>} Table schema info
 */
export const getTableSchema = async (tableName) => {
    const cacheKey = `schema:${tableName}`;

    // Check memory cache first
    if (schemaCache.has(cacheKey)) {
        return schemaCache.get(cacheKey);
    }

    // Check Redis cache
    let schema = await cacheService.get(cacheKey);

    if (!schema) {
        const startTime = Date.now();

        try {
            // Get column information from information_schema
            const schemaQuery = `
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM information_schema.columns 
                WHERE table_name = $1 
                AND table_schema = COALESCE(CURRENT_SCHEMA(), 'public')
                ORDER BY ordinal_position
            `;

            const result = await query(schemaQuery, [tableName]);

            if (result.rows.length === 0) {
                throw new Error(`Table '${tableName}' not found`);
            }

            schema = {
                tableName,
                columns: result.rows.reduce((acc, row) => {
                    acc[row.column_name] = {
                        type: row.data_type,
                        nullable: row.is_nullable === 'YES',
                        default: row.column_default,
                        maxLength: row.character_maximum_length,
                        precision: row.numeric_precision,
                        scale: row.numeric_scale
                    };
                    return acc;
                }, {}),
                hasDeletedAt: result.rows.some(row => row.column_name === 'deleted_at'),
                hasCreatedAt: result.rows.some(row => row.column_name === 'created_at'),
                hasUpdatedAt: result.rows.some(row => row.column_name === 'updated_at'),
                hasVersion: result.rows.some(row => row.column_name === 'version'),
                primaryKey: null, // Will be populated separately if needed
                lastUpdated: new Date()
            };

            // Get primary key information
            const pkQuery = `
                SELECT column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = $1 
                AND tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = COALESCE(CURRENT_SCHEMA(), 'public')
            `;

            const pkResult = await query(pkQuery, [tableName]);
            if (pkResult.rows.length > 0) {
                schema.primaryKey = pkResult.rows.map(row => row.column_name);
            }

            // Cache schema for 1 hour
            await cacheService.set(cacheKey, schema, 3600);
            schemaCache.set(cacheKey, schema);

            logDatabaseOperation('getTableSchema', schemaQuery, [tableName], startTime, result);

        } catch (error) {
            logDatabaseOperation('getTableSchema', `SCHEMA ${tableName}`, [tableName], startTime, null, error);
            throw error;
        }
    } else {
        // Update memory cache from Redis
        schemaCache.set(cacheKey, schema);
    }

    return schema;
};

/**
 * Clear schema cache for a table
 * @param {string} tableName - Table name
 */
const clearSchemaCache = async (tableName) => {
    const cacheKey = `schema:${tableName}`;
    schemaCache.delete(cacheKey);
    await cacheService.delete(cacheKey);
};

/**
 * Generate cache keys for table operations
 * @param {string} tableName - Table name
 * @param {Object} conditions - Query conditions
 * @returns {Array} Array of cache keys to invalidate
 */
const generateCacheKeys = (tableName, conditions = {}) => {
    const keys = [
        `table:${tableName}:*`,
        `count:${tableName}:*`,
        `top:${tableName}:*`,
        `paginated:${tableName}:*`
    ];

    // Add specific condition-based keys
    if (Object.keys(conditions).length > 0) {
        const conditionKey = Object.keys(conditions).sort().join('_');
        keys.push(`query:${tableName}:${conditionKey}:*`);
    }

    return keys;
};

/**
 * Invalidate cache after data modifications
 * @param {string} tableName - Table name
 * @param {string} operation - Operation type (insert, update, delete)
 * @param {Object} conditions - Conditions used
 */
const invalidateCache = async (tableName, operation, conditions = {}) => {
    try {
        const cacheKeys = generateCacheKeys(tableName, conditions);

        // Use tags for efficient cache invalidation
        const tags = [
            `table:${tableName}`,
            `operation:${operation}`,
            'data_modification'
        ];

        await cacheService.invalidateByTags(tags);

        debugDb(`Cache invalidated for ${tableName} after ${operation}`);
    } catch (error) {
        logger.warn('Cache invalidation failed:', { error: error.message, tableName, operation });
    }
};

/**
 * Execute query with timeout support
 */
const executeWithTimeout = async (queryText, params, timeout) => {
    return new Promise((resolve, reject) => {
        let timeoutId;
        let isResolved = false;

        if (timeout > 0) {
            timeoutId = setTimeout(() => {
                if (!isResolved) {
                    isResolved = true;
                    reject(new Error(`Query timeout after ${timeout}ms: ${queryText.substring(0, 100)}...`));
                }
            }, timeout);
        }

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
 * Enhanced paginated query with schema awareness and caching
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
            orderBy = null,
            orderDirection = 'DESC',
            includeCount = true,
            maxLimit = 100,
            timeout = 30000,
            tableName = null,
            useCache = false,
            cacheTTL = 300 // 5 minutes
        } = paginationOptions;

        // Validate and sanitize inputs
        const validatedPage = Math.max(1, parseInt(page));
        const validatedLimit = Math.min(Math.max(1, parseInt(limit)), maxLimit);
        const offset = (validatedPage - 1) * validatedLimit;

        // Validate order direction
        const validOrderDirection = ['ASC', 'DESC'].includes(orderDirection.toUpperCase())
            ? orderDirection.toUpperCase()
            : 'DESC';

        // Generate cache key if caching is enabled
        let cacheKey = null;
        if (useCache && tableName) {
            const queryHash = Buffer.from(`${baseQuery}${JSON.stringify(params)}`).toString('base64').substring(0, 16);
            cacheKey = `paginated:${tableName}:${queryHash}:${validatedPage}:${validatedLimit}:${orderBy}:${validOrderDirection}`;

            const cached = await cacheService.get(cacheKey);
            if (cached) {
                debugDb(`Cache hit for paginated query: ${cacheKey}`);
                return cached;
            }
        }

        let total = 0;

        if (includeCount) {
            const countQuery = `SELECT COUNT(*) as total FROM (${baseQuery}) as count_query`;
            const countResult = timeout > 0
                ? await executeWithTimeout(countQuery, params, timeout)
                : await query(countQuery, params);
            total = parseInt(countResult.rows[0].total);
        }

        // Build data query with dynamic order by
        let dataQuery = baseQuery;

        if (orderBy) {
            // Validate orderBy column exists if tableName provided
            if (tableName) {
                const schema = await getTableSchema(tableName);
                if (!schema.columns[orderBy]) {
                    logger.warn(`Order by column '${orderBy}' not found in table '${tableName}', using default`);
                    // Use first column or primary key as fallback
                    const fallbackColumn = schema.primaryKey?.[0] || Object.keys(schema.columns)[0];
                    dataQuery += ` ORDER BY ${fallbackColumn} ${validOrderDirection}`;
                } else {
                    dataQuery += ` ORDER BY ${orderBy} ${validOrderDirection}`;
                }
            } else {
                dataQuery += ` ORDER BY ${orderBy} ${validOrderDirection}`;
            }
        }

        dataQuery += ` LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;

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

        // Cache result if caching is enabled
        if (useCache && cacheKey) {
            await cacheService.set(cacheKey, result, cacheTTL, [`table:${tableName}`]);
            debugDb(`Cached paginated query result: ${cacheKey}`);
        }

        logDatabaseOperation('paginatedQuery', dataQuery, [...params, validatedLimit, offset], startTime, dataResult);
        return result;

    } catch (error) {
        logDatabaseOperation('paginatedQuery', baseQuery, params, startTime, null, error);
        throw error;
    }
};

/**
 * Schema-aware insert function with cache invalidation
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
            conflictColumns = null,
            returning = '*',
            validate = true
        } = options;

        if (validate && (!data || Object.keys(data).length === 0)) {
            throw new Error('Insert data cannot be empty');
        }

        // Get table schema
        const schema = await getTableSchema(tableName);

        // Filter out columns that don't exist in the table
        const validData = {};
        Object.entries(data).forEach(([key, value]) => {
            if (schema.columns[key]) {
                validData[key] = value;
            } else if (validate) {
                logger.warn(`Column '${key}' not found in table '${tableName}', skipping`);
            }
        });

        // Auto-add timestamp columns if they exist and aren't provided
        if (schema.hasCreatedAt && !validData.created_at) {
            validData.created_at = new Date();
        }
        if (schema.hasUpdatedAt && !validData.updated_at) {
            validData.updated_at = new Date();
        }

        const columns = Object.keys(validData);
        const values = Object.values(validData);
        const placeholders = values.map((_, index) => `$${index + 1}`);

        let queryText = `
            INSERT INTO ${tableName} (${columns.join(', ')})
            VALUES (${placeholders.join(', ')})
        `;

        // Handle conflict resolution
        if (onConflict && conflictColumns) {
            const resolvedConflictColumns = conflictColumns || schema.primaryKey || ['id'];

            if (onConflict === 'ignore') {
                queryText += ` ON CONFLICT (${resolvedConflictColumns.join(', ')}) DO NOTHING`;
            } else if (onConflict === 'update') {
                const updateSet = columns
                    .filter(col => !resolvedConflictColumns.includes(col))
                    .map(col => `${col} = EXCLUDED.${col}`)
                    .join(', ');

                if (updateSet) {
                    queryText += ` ON CONFLICT (${resolvedConflictColumns.join(', ')}) DO UPDATE SET ${updateSet}`;
                    // Add updated_at if exists
                    if (schema.hasUpdatedAt) {
                        queryText = queryText.replace(' DO UPDATE SET ', ' DO UPDATE SET updated_at = NOW(), ');
                    }
                }
            }
        }

        queryText += ` RETURNING ${returning}`;

        const result = await query(queryText, values);

        // Invalidate cache after successful insert
        await invalidateCache(tableName, 'insert', validData);

        logDatabaseOperation('insertRecord', queryText, values, startTime, result);
        return result.rows[0];

    } catch (error) {
        logDatabaseOperation('insertRecord', `INSERT INTO ${tableName}`, Object.values(data), startTime, null, error);
        throw error;
    }
};

/**
 * Schema-aware update function with cache invalidation
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

        // Get table schema
        const schema = await getTableSchema(tableName);

        // Filter out columns that don't exist in the table
        const validData = {};
        Object.entries(data).forEach(([key, value]) => {
            if (schema.columns[key]) {
                validData[key] = value;
            } else {
                logger.warn(`Column '${key}' not found in table '${tableName}', skipping`);
            }
        });

        // Auto-add updated_at if it exists and isn't provided
        if (schema.hasUpdatedAt && !validData.updated_at) {
            validData.updated_at = new Date();
        }

        // Handle optimistic locking
        if (optimisticLocking && schema.hasVersion && validData[versionColumn] !== undefined) {
            whereConditions[versionColumn] = validData[versionColumn];
            validData[versionColumn] = validData[versionColumn] + 1;
        }

        const dataColumns = Object.keys(validData);
        const dataValues = Object.values(validData);
        const whereColumns = Object.keys(whereConditions);
        const whereValues = Object.values(whereConditions);

        const setClause = dataColumns.map((col, index) => `${col} = $${index + 1}`).join(', ');
        const whereClause = whereColumns.map((col, index) => `${col} = $${dataValues.length + index + 1}`).join(' AND ');

        const queryText = `
            UPDATE ${tableName}
            SET ${setClause}
            WHERE ${whereClause}
            RETURNING ${returning}
        `;

        const result = await query(queryText, [...dataValues, ...whereValues]);

        if (result.rows.length === 0) {
            throw new Error(`No records updated in ${tableName}. Record may not exist or version conflict occurred.`);
        }

        // Invalidate cache after successful update
        await invalidateCache(tableName, 'update', whereConditions);

        logDatabaseOperation('updateRecord', queryText, [...dataValues, ...whereValues], startTime, result);
        return result.rows[0];

    } catch (error) {
        logDatabaseOperation('updateRecord', `UPDATE ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Schema-aware delete function with cache invalidation
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
            softDelete = null,
            force = false
        } = options;

        const whereColumns = Object.keys(whereConditions);
        const whereValues = Object.values(whereConditions);

        if (whereColumns.length === 0 && !force) {
            throw new Error('Delete operation requires WHERE conditions or force=true');
        }

        // Get table schema
        const schema = await getTableSchema(tableName);

        // Determine if soft delete should be used
        const useSoftDelete = softDelete !== null ? softDelete : schema.hasDeletedAt;

        const whereClause = whereColumns.length > 0
            ? whereColumns.map((col, index) => `${col} = $${index + 1}`).join(' AND ')
            : '1=1';

        let queryText;

        if (useSoftDelete) {
            // Build soft delete query
            let setClause = 'deleted_at = NOW()';
            if (schema.hasUpdatedAt) {
                setClause += ', updated_at = NOW()';
            }

            queryText = `
                UPDATE ${tableName}
                SET ${setClause}
                WHERE ${whereClause} AND (deleted_at IS NULL OR deleted_at > NOW())
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

        // Invalidate cache after successful delete
        await invalidateCache(tableName, 'delete', whereConditions);

        logDatabaseOperation('deleteRecord', queryText, whereValues, startTime, result);
        return result.rows[0];

    } catch (error) {
        logDatabaseOperation('deleteRecord', `DELETE FROM ${tableName}`, Object.values(whereConditions), startTime, null, error);
        throw error;
    }
};

/**
 * Enhanced advancedQuery with schema awareness
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
            includeSoftDeleted = null,
            useCache = false,
            cacheTTL = 300
        } = options;

        // Get table schema
        const schema = await getTableSchema(tableName);

        // Determine soft delete behavior
        const shouldFilterSoftDeleted = includeSoftDeleted !== null
            ? !includeSoftDeleted
            : schema.hasDeletedAt;

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
            // Validate column exists
            if (!schema.columns[key] && !key.includes('.')) {
                logger.warn(`Column '${key}' not found in table '${tableName}', skipping condition`);
                return;
            }

            if (Array.isArray(value)) {
                const placeholders = value.map(() => `$${paramIndex++}`).join(', ');
                whereClauses.push(`${key} IN (${placeholders})`);
                params.push(...value);
            } else if (value === null) {
                whereClauses.push(`${key} IS NULL`);
            } else {
                whereClauses.push(`${key} = $${paramIndex++}`);
                params.push(value);
            }
        });

        // Raw where clause
        if (whereRaw) {
            whereClauses.push(whereRaw);
        }

        // Soft delete filter (only if table has deleted_at column)
        if (shouldFilterSoftDeleted) {
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

        // ORDER BY clause with validation
        if (orderBy) {
            const [orderColumn] = orderBy.split(' ');
            if (schema.columns[orderColumn] || orderColumn.includes('.')) {
                queryParts.push(`ORDER BY ${orderBy}`);
            } else {
                logger.warn(`Order by column '${orderColumn}' not found in table '${tableName}', skipping`);
            }
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

        // Check cache if enabled
        if (useCache) {
            const queryHash = Buffer.from(`${queryText}${JSON.stringify(params)}`).toString('base64').substring(0, 16);
            const cacheKey = `query:${tableName}:${queryHash}`;

            const cached = await cacheService.get(cacheKey);
            if (cached) {
                debugDb(`Cache hit for advanced query: ${cacheKey}`);
                return cached;
            }

            const result = await query(queryText, params);

            // Cache result
            await cacheService.set(cacheKey, result.rows, cacheTTL, [`table:${tableName}`]);

            logDatabaseOperation('advancedQuery', queryText, params, startTime, result);
            return result.rows;
        }

        const result = await query(queryText, params);

        logDatabaseOperation('advancedQuery', queryText, params, startTime, result);
        return result.rows;

    } catch (error) {
        logDatabaseOperation('advancedQuery', `ADVANCED QUERY ON ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Enhanced getTopRecords with schema awareness
 * @param {string} tableName - Table name
 * @param {Object} options - Top query options
 * @returns {Promise<Array>} Top records
 */
export const getTopRecords = async (tableName, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            limit = 10,
            orderBy = null,
            where = {},
            includeSoftDeleted = null,
            useCache = false,
            cacheTTL = 300
        } = options;

        // Get table schema to determine default order by
        const schema = await getTableSchema(tableName);

        // Use provided orderBy or fall back to created_at, primary key, or first column
        let defaultOrderBy = orderBy;
        if (!defaultOrderBy) {
            if (schema.hasCreatedAt) {
                defaultOrderBy = 'created_at DESC';
            } else if (schema.primaryKey && schema.primaryKey.length > 0) {
                defaultOrderBy = `${schema.primaryKey[0]} DESC`;
            } else {
                const firstColumn = Object.keys(schema.columns)[0];
                defaultOrderBy = `${firstColumn} DESC`;
            }
        }

        return await advancedQuery(tableName, {
            where,
            orderBy: defaultOrderBy,
            limit,
            includeSoftDeleted,
            useCache,
            cacheTTL
        });

    } catch (error) {
        logDatabaseOperation('getTopRecords', `TOP RECORDS FROM ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Schema-aware findById with caching
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
            includeSoftDeleted = null,
            useCache = true,
            cacheTTL = 600
        } = options;

        // Get table schema
        const schema = await getTableSchema(tableName);

        // Determine primary key column
        const pkColumn = schema.primaryKey?.[0] || 'id';

        if (!schema.columns[pkColumn]) {
            throw new Error(`Primary key column '${pkColumn}' not found in table '${tableName}'`);
        }

        // Check cache first if enabled
        if (useCache) {
            const cacheKey = `record:${tableName}:${pkColumn}:${id}`;
            const cached = await cacheService.get(cacheKey);
            if (cached) {
                debugDb(`Cache hit for findById: ${cacheKey}`);
                return cached;
            }
        }

        const whereConditions = { [pkColumn]: id };

        const results = await advancedQuery(tableName, {
            select: columns,
            where: whereConditions,
            limit: 1,
            includeSoftDeleted,
            useCache: false // We handle caching here
        });

        const result = results[0] || null;

        // Cache result if enabled and found
        if (useCache && result) {
            const cacheKey = `record:${tableName}:${pkColumn}:${id}`;
            await cacheService.set(cacheKey, result, cacheTTL, [`table:${tableName}`]);
        }

        logDatabaseOperation('findById', `SELECT FROM ${tableName}`, [id], startTime, { rows: results });
        return result;

    } catch (error) {
        logDatabaseOperation('findById', `SELECT FROM ${tableName}`, [id], startTime, null, error);
        throw error;
    }
};

/**
 * Enhanced count function with schema awareness
 * @param {string} tableName - Table name
 * @param {Object} conditions - Count conditions
 * @param {Object} options - Count options
 * @returns {Promise<number>} Record count
 */
export const countRecords = async (tableName, conditions = {}, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            includeSoftDeleted = null,
            useCache = true,
            cacheTTL = 300
        } = options;

        // Check cache first if enabled
        if (useCache) {
            const conditionsHash = Buffer.from(JSON.stringify(conditions)).toString('base64').substring(0, 16);
            const cacheKey = `count:${tableName}:${conditionsHash}:${includeSoftDeleted}`;
            const cached = await cacheService.get(cacheKey);
            if (cached !== null) {
                debugDb(`Cache hit for countRecords: ${cacheKey}`);
                return cached;
            }
        }

        // Get table schema
        const schema = await getTableSchema(tableName);

        const whereColumns = Object.keys(conditions);
        const whereValues = Object.values(conditions);

        // Validate columns exist
        const validConditions = {};
        Object.entries(conditions).forEach(([key, value]) => {
            if (schema.columns[key]) {
                validConditions[key] = value;
            } else {
                logger.warn(`Column '${key}' not found in table '${tableName}', skipping condition`);
            }
        });

        let whereClauses = [];
        let params = [];
        let paramIndex = 1;

        Object.entries(validConditions).forEach(([key, value]) => {
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

        // Handle soft delete filtering
        const shouldFilterSoftDeleted = includeSoftDeleted !== null
            ? !includeSoftDeleted
            : schema.hasDeletedAt;

        if (shouldFilterSoftDeleted) {
            whereClauses.push('(deleted_at IS NULL OR deleted_at > NOW())');
        }

        const whereClause = whereClauses.length > 0
            ? `WHERE ${whereClauses.join(' AND ')}`
            : '';

        const queryText = `
            SELECT COUNT(*) as count
            FROM ${tableName}
            ${whereClause}
        `;

        const result = await query(queryText, params);
        const count = parseInt(result.rows[0].count);

        // Cache result if enabled
        if (useCache) {
            const conditionsHash = Buffer.from(JSON.stringify(conditions)).toString('base64').substring(0, 16);
            const cacheKey = `count:${tableName}:${conditionsHash}:${includeSoftDeleted}`;
            await cacheService.set(cacheKey, count, cacheTTL, [`table:${tableName}`]);
        }

        logDatabaseOperation('countRecords', queryText, params, startTime, result);
        return count;

    } catch (error) {
        logDatabaseOperation('countRecords', `COUNT FROM ${tableName}`, Object.values(conditions), startTime, null, error);
        throw error;
    }
};

/**
 * Enhanced bulk insert with schema validation and cache invalidation
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
            conflictColumns = null,
            returning = '*',
            timeout = 60000,
            validateSchema = true
        } = options;

        if (!Array.isArray(dataArray) || dataArray.length === 0) {
            throw new Error('Data array cannot be empty');
        }

        // Get table schema if validation is enabled
        let schema = null;
        if (validateSchema) {
            schema = await getTableSchema(tableName);
        }

        const results = [];

        // Execute with timeout
        await executeTransaction(async (client) => {
            // Process in batches
            for (let i = 0; i < dataArray.length; i += batchSize) {
                const batch = dataArray.slice(i, i + batchSize);

                for (const data of batch) {
                    let validData = data;

                    // Validate and filter data if schema validation is enabled
                    if (schema) {
                        validData = {};
                        Object.entries(data).forEach(([key, value]) => {
                            if (schema.columns[key]) {
                                validData[key] = value;
                            } else {
                                logger.warn(`Column '${key}' not found in table '${tableName}', skipping`);
                            }
                        });

                        // Auto-add timestamp columns if they exist and aren't provided
                        if (schema.hasCreatedAt && !validData.created_at) {
                            validData.created_at = new Date();
                        }
                        if (schema.hasUpdatedAt && !validData.updated_at) {
                            validData.updated_at = new Date();
                        }
                    }

                    const result = await insertRecord(tableName, validData, {
                        onConflict,
                        conflictColumns: conflictColumns || schema?.primaryKey,
                        returning,
                        validate: false // Skip individual validation in bulk
                    });
                    if (result) results.push(result);
                }
            }

            return results;
        }, { timeout });

        // Invalidate cache after successful bulk insert
        await invalidateCache(tableName, 'bulkInsert');

        logDatabaseOperation('bulkInsert', `BULK INSERT INTO ${tableName}`, [`${dataArray.length} records`], startTime, { rowCount: results.length });
        return results;

    } catch (error) {
        logDatabaseOperation('bulkInsert', `BULK INSERT INTO ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Enhanced upsert with schema awareness
 * @param {string} tableName - Table name
 * @param {Object} data - Data to upsert
 * @param {Array} conflictColumns - Columns to check for conflict
 * @param {Object} options - Upsert options
 * @returns {Promise<Object>} Upsert result
 */
export const upsertRecord = async (tableName, data, conflictColumns = null, options = {}) => {
    const startTime = Date.now();

    try {
        const { returning = '*', excludeFromUpdate = [] } = options;

        // Get table schema
        const schema = await getTableSchema(tableName);

        // Use provided conflict columns or fall back to primary key
        const resolvedConflictColumns = conflictColumns || schema.primaryKey || ['id'];

        // Validate conflict columns exist
        resolvedConflictColumns.forEach(col => {
            if (!schema.columns[col]) {
                throw new Error(`Conflict column '${col}' not found in table '${tableName}'`);
            }
        });

        // Filter out columns that don't exist
        const validData = {};
        Object.entries(data).forEach(([key, value]) => {
            if (schema.columns[key]) {
                validData[key] = value;
            } else {
                logger.warn(`Column '${key}' not found in table '${tableName}', skipping`);
            }
        });

        // Auto-add timestamp columns
        if (schema.hasCreatedAt && !validData.created_at) {
            validData.created_at = new Date();
        }
        if (schema.hasUpdatedAt && !validData.updated_at) {
            validData.updated_at = new Date();
        }

        const columns = Object.keys(validData);
        const values = Object.values(validData);
        const placeholders = values.map((_, index) => `${index + 1}`);

        const updateColumns = columns.filter(col =>
            !resolvedConflictColumns.includes(col) && !excludeFromUpdate.includes(col)
        );

        let updateSet = updateColumns.map(col => `${col} = EXCLUDED.${col}`).join(', ');

        // Add updated_at to update clause if exists and not excluded
        if (schema.hasUpdatedAt && !excludeFromUpdate.includes('updated_at')) {
            updateSet = updateSet ? `updated_at = NOW(), ${updateSet}` : 'updated_at = NOW()';
        }

        const queryText = `
            INSERT INTO ${tableName} (${columns.join(', ')})
            VALUES (${placeholders.join(', ')})
            ON CONFLICT (${resolvedConflictColumns.join(', ')}) 
            DO UPDATE SET ${updateSet}
            RETURNING ${returning}
        `;

        const result = await query(queryText, values);

        // Invalidate cache after successful upsert
        await invalidateCache(tableName, 'upsert', validData);

        logDatabaseOperation('upsertRecord', queryText, values, startTime, result);
        return result.rows[0];

    } catch (error) {
        logDatabaseOperation('upsertRecord', `UPSERT ${tableName}`, Object.values(data), startTime, null, error);
        throw error;
    }
};

/**
 * Enhanced executeTransaction with better error handling and logging
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
 * Database health check with enhanced validation
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
            responseTime: duration,
            cacheStatus: schemaCache.size > 0 ? 'active' : 'empty'
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
 * Find records with enhanced search capabilities
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
            orderBy = null,
            limit = null,
            includeSoftDeleted = null,
            useCache = false,
            cacheTTL = 300
        } = options;

        // Get table schema
        const schema = await getTableSchema(tableName);

        // Determine default order by
        let defaultOrderBy = orderBy;
        if (!defaultOrderBy) {
            if (schema.hasCreatedAt) {
                defaultOrderBy = 'created_at DESC';
            } else if (schema.primaryKey && schema.primaryKey.length > 0) {
                defaultOrderBy = `${schema.primaryKey[0]} DESC`;
            }
        }

        return await advancedQuery(tableName, {
            select: columns,
            where: conditions,
            orderBy: defaultOrderBy,
            limit,
            includeSoftDeleted,
            useCache,
            cacheTTL
        });

    } catch (error) {
        logDatabaseOperation('findWhere', `SELECT FROM ${tableName}`, Object.values(conditions), startTime, null, error);
        throw error;
    }
};

/**
 * Verify table exists and optionally create basic indexes
 * @param {string} tableName - Table name to verify
 * @param {Object} options - Verification options
 * @returns {Promise<Object>} Verification result
 */
export const verifyTable = async (tableName, options = {}) => {
    const startTime = Date.now();

    try {
        const { createIndexes = false } = options;

        // Check if table exists
        const tableQuery = `
            SELECT table_name, table_schema
            FROM information_schema.tables 
            WHERE table_name = $1 
            AND table_schema = COALESCE(CURRENT_SCHEMA(), 'public')
        `;

        const tableResult = await query(tableQuery, [tableName]);

        if (tableResult.rows.length === 0) {
            return {
                exists: false,
                tableName,
                error: `Table '${tableName}' not found`
            };
        }

        // Get table schema
        const schema = await getTableSchema(tableName);

        // Get existing indexes
        const indexQuery = `
            SELECT indexname, indexdef
            FROM pg_indexes 
            WHERE tablename = $1 
            AND schemaname = COALESCE(CURRENT_SCHEMA(), 'public')
        `;

        const indexResult = await query(indexQuery, [tableName]);

        const result = {
            exists: true,
            tableName,
            schema,
            indexes: indexResult.rows,
            recommendations: []
        };

        // Add recommendations
        if (schema.hasCreatedAt && !indexResult.rows.some(idx => idx.indexdef.includes('created_at'))) {
            result.recommendations.push('Consider adding index on created_at column for better performance');
        }

        if (schema.hasDeletedAt && !indexResult.rows.some(idx => idx.indexdef.includes('deleted_at'))) {
            result.recommendations.push('Consider adding index on deleted_at column for soft delete queries');
        }

        logDatabaseOperation('verifyTable', tableQuery, [tableName], startTime, tableResult);
        return result;

    } catch (error) {
        logDatabaseOperation('verifyTable', `VERIFY ${tableName}`, [tableName], startTime, null, error);
        throw error;
    }
};

// Export all functions
export {
    logDatabaseOperation,
    executeWithTimeout,
    invalidateCache,
    generateCacheKeys,
    clearSchemaCache
};