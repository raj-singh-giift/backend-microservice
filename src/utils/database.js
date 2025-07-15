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
                primaryKey: null,
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
 * Get stored procedure information with caching
 * @param {string} procedureName - Procedure name
 * @returns {Promise<Object>} Procedure info
 */
const getProcedureSchema = async (procedureName) => {
    const cacheKey = `procedure:${procedureName}`;

    // Check memory cache first
    if (schemaCache.has(cacheKey)) {
        return schemaCache.get(cacheKey);
    }

    // Check Redis cache
    let schema = await cacheService.get(cacheKey);

    if (!schema) {
        const startTime = Date.now();

        try {
            // Get procedure information from information_schema
            const procedureQuery = `
                SELECT 
                    p.specific_name,
                    p.routine_name,
                    p.routine_type,
                    p.data_type as return_type,
                    p.type_udt_name,
                    p.routine_definition,
                    COALESCE(
                        array_agg(
                            CASE 
                                WHEN pp.parameter_name IS NOT NULL 
                                THEN json_build_object(
                                    'name', pp.parameter_name,
                                    'type', pp.data_type,
                                    'mode', pp.parameter_mode,
                                    'position', pp.ordinal_position
                                )
                                ELSE NULL
                            END
                            ORDER BY pp.ordinal_position
                        ) FILTER (WHERE pp.parameter_name IS NOT NULL),
                        ARRAY[]::json[]
                    ) as parameters
                FROM information_schema.routines p
                LEFT JOIN information_schema.parameters pp 
                    ON p.specific_name = pp.specific_name
                WHERE p.routine_name = $1
                AND p.routine_schema = COALESCE(CURRENT_SCHEMA(), 'public')
                GROUP BY p.specific_name, p.routine_name, p.routine_type, 
                         p.data_type, p.type_udt_name, p.routine_definition
            `;

            const result = await query(procedureQuery, [procedureName]);

            if (result.rows.length === 0) {
                throw new Error(`Procedure '${procedureName}' not found`);
            }

            const procedureInfo = result.rows[0];

            schema = {
                procedureName,
                specificName: procedureInfo.specific_name,
                routineType: procedureInfo.routine_type, // FUNCTION or PROCEDURE
                returnType: procedureInfo.return_type,
                parameters: procedureInfo.parameters || [],
                parameterCount: procedureInfo.parameters ? procedureInfo.parameters.length : 0,
                hasInputParams: procedureInfo.parameters.some(p => p.mode === 'IN' || p.mode === 'INOUT'),
                hasOutputParams: procedureInfo.parameters.some(p => p.mode === 'OUT' || p.mode === 'INOUT'),
                isFunction: procedureInfo.routine_type === 'FUNCTION',
                isProcedure: procedureInfo.routine_type === 'PROCEDURE',
                lastUpdated: new Date()
            };

            // Cache schema for 1 hour
            await cacheService.set(cacheKey, schema, 3600);
            schemaCache.set(cacheKey, schema);

            logDatabaseOperation('getProcedureSchema', procedureQuery, [procedureName], startTime, result);

        } catch (error) {
            logDatabaseOperation('getProcedureSchema', `PROCEDURE SCHEMA ${procedureName}`, [procedureName], startTime, null, error);
            throw error;
        }
    } else {
        // Update memory cache from Redis
        schemaCache.set(cacheKey, schema);
    }

    return schema;
};

/**
 * Clear schema cache for a table or procedure
 * @param {string} name - Table or procedure name
 * @param {string} type - 'table' or 'procedure'
 */
const clearSchemaCache = async (name, type = 'table') => {
    const cacheKey = type === 'procedure' ? `procedure:${name}` : `schema:${name}`;
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
 * Execute stored procedure with enhanced error handling and logging
 * @param {string} procedureName - Stored procedure name
 * @param {Array} params - Procedure parameters
 * @param {Object} options - Execution options
 * @returns {Promise<Object>} Procedure result
 */
export const executeStoredProcedure = async (procedureName, params = [], options = {}) => {
    const startTime = Date.now();

    try {
        const {
            validateParams = true,
            timeout = 30000,
            useCache = false,
            cacheTTL = 300
        } = options;

        // Get procedure schema for validation if enabled
        let procedureSchema = null;
        if (validateParams) {
            procedureSchema = await getProcedureSchema(procedureName);

            // Validate parameter count
            const expectedParams = procedureSchema.parameters.filter(p => p.mode === 'IN' || p.mode === 'INOUT');
            if (params.length !== expectedParams.length) {
                throw new Error(
                    `Parameter count mismatch for procedure '${procedureName}'. ` +
                    `Expected ${expectedParams.length}, got ${params.length}`
                );
            }
        }

        // Check cache if enabled
        if (useCache) {
            const cacheKey = `procedure:${procedureName}:${Buffer.from(JSON.stringify(params)).toString('base64').substring(0, 16)}`;
            const cached = await cacheService.get(cacheKey);
            if (cached) {
                debugDb(`Cache hit for stored procedure: ${cacheKey}`);
                return cached;
            }
        }

        const placeholders = params.map((_, index) => `$${index + 1}`).join(', ');
        const queryText = `CALL ${procedureName}(${placeholders})`;

        const result = timeout > 0
            ? await executeWithTimeout(queryText, params, timeout)
            : await query(queryText, params);

        // Cache result if enabled and procedure is deterministic
        if (useCache && procedureSchema?.isFunction) {
            const cacheKey = `procedure:${procedureName}:${Buffer.from(JSON.stringify(params)).toString('base64').substring(0, 16)}`;
            await cacheService.set(cacheKey, result, cacheTTL, [`procedure:${procedureName}`]);
        }

        logDatabaseOperation('executeStoredProcedure', queryText, params, startTime, result);
        return result;

    } catch (error) {
        logDatabaseOperation('executeStoredProcedure', `CALL ${procedureName}`, params, startTime, null, error);
        throw error;
    }
};

/**
 * Execute function (for PostgreSQL functions that return values)
 * @param {string} functionName - Function name
 * @param {Array} params - Function parameters
 * @param {Object} options - Execution options
 * @returns {Promise<Object>} Function result
 */
export const executeFunction = async (functionName, params = [], options = {}) => {
    const startTime = Date.now();

    try {
        const {
            validateParams = true,
            timeout = 30000,
            useCache = false,
            cacheTTL = 300,
            returnType = 'table' // 'table', 'scalar', 'record'
        } = options;

        // Get function schema for validation if enabled
        let functionSchema = null;
        if (validateParams) {
            functionSchema = await getProcedureSchema(functionName);

            if (!functionSchema.isFunction) {
                throw new Error(`'${functionName}' is not a function`);
            }

            // Validate parameter count
            const expectedParams = functionSchema.parameters.filter(p => p.mode === 'IN' || p.mode === 'INOUT');
            if (params.length !== expectedParams.length) {
                throw new Error(
                    `Parameter count mismatch for function '${functionName}'. ` +
                    `Expected ${expectedParams.length}, got ${params.length}`
                );
            }
        }

        // Check cache if enabled
        if (useCache) {
            const cacheKey = `function:${functionName}:${Buffer.from(JSON.stringify(params)).toString('base64').substring(0, 16)}`;
            const cached = await cacheService.get(cacheKey);
            if (cached) {
                debugDb(`Cache hit for function: ${cacheKey}`);
                return cached;
            }
        }

        let queryText;
        const placeholders = params.map((_, index) => `$${index + 1}`).join(', ');

        if (returnType === 'scalar') {
            queryText = `SELECT ${functionName}(${placeholders}) as result`;
        } else if (returnType === 'record') {
            queryText = `SELECT ${functionName}(${placeholders}) as result`;
        } else {
            // Default to table-valued function
            queryText = `SELECT * FROM ${functionName}(${placeholders})`;
        }

        const result = timeout > 0
            ? await executeWithTimeout(queryText, params, timeout)
            : await query(queryText, params);

        // Process result based on return type
        let processedResult = result;
        if (returnType === 'scalar' && result.rows.length > 0) {
            processedResult = {
                ...result,
                value: result.rows[0].result,
                rows: result.rows
            };
        }

        // Cache result if enabled
        if (useCache) {
            const cacheKey = `function:${functionName}:${Buffer.from(JSON.stringify(params)).toString('base64').substring(0, 16)}`;
            await cacheService.set(cacheKey, processedResult, cacheTTL, [`function:${functionName}`]);
        }

        logDatabaseOperation('executeFunction', queryText, params, startTime, result);
        return processedResult;

    } catch (error) {
        logDatabaseOperation('executeFunction', `SELECT ${functionName}`, params, startTime, null, error);
        throw error;
    }
};

/**
 * Execute batch of stored procedures in a transaction
 * @param {Array} procedures - Array of procedure calls {name, params, options}
 * @param {Object} transactionOptions - Transaction options
 * @returns {Promise<Array>} Array of results
 */
export const executeBatchProcedures = async (procedures, transactionOptions = {}) => {
    const startTime = Date.now();

    try {
        const { timeout = 60000, isolationLevel = null } = transactionOptions;

        if (!Array.isArray(procedures) || procedures.length === 0) {
            throw new Error('Procedures array cannot be empty');
        }

        const results = [];

        await executeTransaction(async (client) => {
            // Set isolation level if specified
            if (isolationLevel) {
                await client.query(`SET TRANSACTION ISOLATION LEVEL ${isolationLevel}`);
            }

            for (const proc of procedures) {
                const { name, params = [], options = {} } = proc;

                // Validate procedure exists
                if (options.validateParams !== false) {
                    await getProcedureSchema(name);
                }

                const placeholders = params.map((_, index) => `$${index + 1}`).join(', ');
                const queryText = `CALL ${name}(${placeholders})`;

                const result = await client.query(queryText, params);
                results.push({
                    procedureName: name,
                    result,
                    params
                });

                debugDb(`Executed procedure in batch: ${name}`);
            }

            return results;
        }, { timeout });

        logDatabaseOperation('executeBatchProcedures', `BATCH ${procedures.length} procedures`, [], startTime, { rowCount: results.length });
        return results;

    } catch (error) {
        logDatabaseOperation('executeBatchProcedures', 'BATCH PROCEDURES', [], startTime, null, error);
        throw error;
    }
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
            cacheTTL = 300
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
            ? whereColumns.map((col, index) => `${col} = ${index + 1}`).join(' AND ')
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

/**
 * Get stored procedure/function list with metadata
 * @param {Object} options - Query options
 * @returns {Promise<Array>} List of procedures/functions
 */
export const listProceduresAndFunctions = async (options = {}) => {
    const startTime = Date.now();

    try {
        const {
            routineType = null, // 'FUNCTION', 'PROCEDURE', or null for both
            schema = 'public'
        } = options;

        let whereClause = 'WHERE r.routine_schema = $1';
        const params = [schema];

        if (routineType) {
            whereClause += ' AND r.routine_type = $2';
            params.push(routineType);
        }

        const listQuery = `
            SELECT 
                r.routine_name,
                r.routine_type,
                r.data_type as return_type,
                r.routine_definition,
                COUNT(p.parameter_name) as parameter_count,
                array_agg(
                    CASE 
                        WHEN p.parameter_name IS NOT NULL 
                        THEN json_build_object(
                            'name', p.parameter_name,
                            'type', p.data_type,
                            'mode', p.parameter_mode
                        )
                        ELSE NULL
                    END
                ) FILTER (WHERE p.parameter_name IS NOT NULL) as parameters
            FROM information_schema.routines r
            LEFT JOIN information_schema.parameters p 
                ON r.specific_name = p.specific_name
            ${whereClause}
            GROUP BY r.routine_name, r.routine_type, r.data_type, r.routine_definition
            ORDER BY r.routine_type, r.routine_name
        `;

        const result = await query(listQuery, params);

        logDatabaseOperation('listProceduresAndFunctions', listQuery, params, startTime, result);
        return result.rows;

    } catch (error) {
        logDatabaseOperation('listProceduresAndFunctions', 'LIST PROCEDURES/FUNCTIONS', [], startTime, null, error);
        throw error;
    }
};

/**
 * Execute dynamic SQL with parameter validation
 * @param {string} sqlTemplate - SQL template with placeholders
 * @param {Object} params - Named parameters
 * @param {Object} options - Execution options
 * @returns {Promise<Object>} Query result
 */
export const executeDynamicSQL = async (sqlTemplate, params = {}, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            allowedTables = [],
            allowedColumns = {},
            timeout = 30000,
            validateSQL = true
        } = options;

        if (validateSQL) {
            // Basic SQL injection prevention
            const suspiciousPatterns = [
                /;\s*drop\s+/i,
                /;\s*delete\s+/i,
                /;\s*truncate\s+/i,
                /;\s*alter\s+/i,
                /--/,
                /\/\*/,
                /\*\//
            ];

            for (const pattern of suspiciousPatterns) {
                if (pattern.test(sqlTemplate)) {
                    throw new Error('Potentially dangerous SQL detected');
                }
            }

            // Validate table names if specified
            if (allowedTables.length > 0) {
                const tablePattern = /FROM\s+(\w+)|JOIN\s+(\w+)|UPDATE\s+(\w+)|INSERT\s+INTO\s+(\w+)/gi;
                let match;
                while ((match = tablePattern.exec(sqlTemplate)) !== null) {
                    const tableName = match[1] || match[2] || match[3] || match[4];
                    if (!allowedTables.includes(tableName)) {
                        throw new Error(`Table '${tableName}' not in allowed tables list`);
                    }
                }
            }
        }

        // Replace named parameters with positional parameters
        let finalSQL = sqlTemplate;
        const paramValues = [];
        let paramIndex = 1;

        Object.entries(params).forEach(([key, value]) => {
            const placeholder = `:${key}`;
            while (finalSQL.includes(placeholder)) {
                finalSQL = finalSQL.replace(placeholder, `${paramIndex}`);
                paramValues.push(value);
                paramIndex++;
            }
        });

        const result = timeout > 0
            ? await executeWithTimeout(finalSQL, paramValues, timeout)
            : await query(finalSQL, paramValues);

        logDatabaseOperation('executeDynamicSQL', finalSQL, paramValues, startTime, result);
        return result;

    } catch (error) {
        logDatabaseOperation('executeDynamicSQL', sqlTemplate, Object.values(params), startTime, null, error);
        throw error;
    }
};

/**
 * Execute raw SQL with enhanced safety checks
 * @param {string} sqlQuery - Raw SQL query
 * @param {Array} params - Query parameters
 * @param {Object} options - Execution options
 * @returns {Promise<Object>} Query result
 */
export const executeRawSQL = async (sqlQuery, params = [], options = {}) => {
    const startTime = Date.now();

    try {
        const {
            allowDangerous = false,
            timeout = 30000,
            returnFirst = false
        } = options;

        if (!allowDangerous) {
            // Basic safety checks
            const dangerousPatterns = [
                /drop\s+table/i,
                /drop\s+database/i,
                /truncate/i,
                /delete\s+from.*without\s+where/i
            ];

            for (const pattern of dangerousPatterns) {
                if (pattern.test(sqlQuery)) {
                    throw new Error('Dangerous SQL operation detected. Use allowDangerous: true to override.');
                }
            }
        }

        const result = timeout > 0
            ? await executeWithTimeout(sqlQuery, params, timeout)
            : await query(sqlQuery, params);

        logDatabaseOperation('executeRawSQL', sqlQuery, params, startTime, result);

        if (returnFirst && result.rows.length > 0) {
            return { ...result, data: result.rows[0] };
        }

        return result;

    } catch (error) {
        logDatabaseOperation('executeRawSQL', sqlQuery, params, startTime, null, error);
        throw error;
    }
};

/**
 * Bulk update records with schema validation
 * @param {string} tableName - Table name
 * @param {Array} updates - Array of update objects {where, data}
 * @param {Object} options - Update options
 * @returns {Promise<Array>} Array of updated records
 */
export const bulkUpdate = async (tableName, updates, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            batchSize = 500,
            timeout = 60000,
            validateSchema = true,
            returning = '*'
        } = options;

        if (!Array.isArray(updates) || updates.length === 0) {
            throw new Error('Updates array cannot be empty');
        }

        // Get table schema if validation is enabled
        let schema = null;
        if (validateSchema) {
            schema = await getTableSchema(tableName);
        }

        const results = [];

        await executeTransaction(async (client) => {
            // Process in batches
            for (let i = 0; i < updates.length; i += batchSize) {
                const batch = updates.slice(i, i + batchSize);

                for (const update of batch) {
                    const { where, data } = update;

                    if (!where || !data) {
                        throw new Error('Each update must have "where" and "data" properties');
                    }

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

                        // Auto-add updated_at if it exists and isn't provided
                        if (schema.hasUpdatedAt && !validData.updated_at) {
                            validData.updated_at = new Date();
                        }
                    }

                    const result = await updateRecord(tableName, validData, where, {
                        returning,
                        validate: false // Skip individual validation in bulk
                    });

                    if (result) results.push(result);
                }
            }

            return results;
        }, { timeout });

        // Invalidate cache after successful bulk update
        await invalidateCache(tableName, 'bulkUpdate');

        logDatabaseOperation('bulkUpdate', `BULK UPDATE ${tableName}`, [`${updates.length} records`], startTime, { rowCount: results.length });
        return results;

    } catch (error) {
        logDatabaseOperation('bulkUpdate', `BULK UPDATE ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Execute aggregation queries with caching
 * @param {string} tableName - Table name
 * @param {Object} aggregations - Aggregation definitions
 * @param {Object} options - Query options
 * @returns {Promise<Object>} Aggregation results
 */
export const executeAggregation = async (tableName, aggregations, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            where = {},
            groupBy = null,
            having = null,
            includeSoftDeleted = null,
            useCache = true,
            cacheTTL = 600
        } = options;

        // Check cache if enabled
        if (useCache) {
            const cacheKey = `aggregation:${tableName}:${Buffer.from(JSON.stringify({ aggregations, where, groupBy, having, includeSoftDeleted })).toString('base64').substring(0, 16)}`;
            const cached = await cacheService.get(cacheKey);
            if (cached) {
                debugDb(`Cache hit for aggregation: ${cacheKey}`);
                return cached;
            }
        }

        // Get table schema
        const schema = await getTableSchema(tableName);

        // Build aggregation SELECT clause
        const selectParts = [];
        Object.entries(aggregations).forEach(([alias, config]) => {
            const { function: func, column, distinct = false } = config;

            if (!schema.columns[column] && column !== '*') {
                throw new Error(`Column '${column}' not found in table '${tableName}'`);
            }

            const distinctClause = distinct ? 'DISTINCT ' : '';
            selectParts.push(`${func}(${distinctClause}${column}) as ${alias}`);
        });

        // Add GROUP BY columns to SELECT if specified
        if (groupBy) {
            const groupColumns = Array.isArray(groupBy) ? groupBy : [groupBy];
            groupColumns.forEach(col => {
                if (!selectParts.some(part => part.includes(col))) {
                    selectParts.push(col);
                }
            });
        }

        const result = await advancedQuery(tableName, {
            select: selectParts.join(', '),
            where,
            groupBy: Array.isArray(groupBy) ? groupBy.join(', ') : groupBy,
            having,
            includeSoftDeleted,
            useCache: false // We handle caching here
        });

        // Cache result if enabled
        if (useCache) {
            const cacheKey = `aggregation:${tableName}:${Buffer.from(JSON.stringify({ aggregations, where, groupBy, having, includeSoftDeleted })).toString('base64').substring(0, 16)}`;
            await cacheService.set(cacheKey, result, cacheTTL, [`table:${tableName}`]);
        }

        logDatabaseOperation('executeAggregation', `AGGREGATION ON ${tableName}`, [], startTime, { rows: result });
        return result;

    } catch (error) {
        logDatabaseOperation('executeAggregation', `AGGREGATION ON ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Execute full-text search with ranking
 * @param {string} tableName - Table name
 * @param {string} searchTerm - Search term
 * @param {Object} options - Search options
 * @returns {Promise<Array>} Search results with ranking
 */
export const executeFullTextSearch = async (tableName, searchTerm, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            searchColumns = [],
            limit = 50,
            offset = 0,
            minRank = 0.1,
            language = 'english',
            includeSoftDeleted = null,
            useCache = false,
            cacheTTL = 300
        } = options;

        if (!searchTerm || searchTerm.trim().length === 0) {
            throw new Error('Search term cannot be empty');
        }

        // Get table schema
        const schema = await getTableSchema(tableName);

        // Validate search columns
        const validSearchColumns = searchColumns.filter(col => {
            if (schema.columns[col]) {
                return true;
            } else {
                logger.warn(`Search column '${col}' not found in table '${tableName}', skipping`);
                return false;
            }
        });

        if (validSearchColumns.length === 0) {
            throw new Error('No valid search columns provided');
        }

        // Build full-text search query
        const searchVector = validSearchColumns.map(col => `to_tsvector('${language}', COALESCE(${col}, ''))`).join(' || ');
        const searchQuery = `to_tsquery('${language}', $1)`;

        let params = [searchTerm.trim().replace(/\s+/g, ' & ')];
        let paramIndex = 2;

        // Build WHERE clause
        let whereClauses = [`${searchVector} @@ ${searchQuery}`];

        // Add rank filter
        if (minRank > 0) {
            whereClauses.push(`ts_rank(${searchVector}, ${searchQuery}) >= ${paramIndex++}`);
            params.push(minRank);
        }

        // Handle soft delete filtering
        const shouldFilterSoftDeleted = includeSoftDeleted !== null
            ? !includeSoftDeleted
            : schema.hasDeletedAt;

        if (shouldFilterSoftDeleted) {
            whereClauses.push('(deleted_at IS NULL OR deleted_at > NOW())');
        }

        const whereClause = whereClauses.join(' AND ');

        const queryText = `
            SELECT *,
                   ts_rank(${searchVector}, ${searchQuery}) as search_rank,
                   ts_headline('${language}', ${validSearchColumns[0]}, ${searchQuery}) as highlight
            FROM ${tableName}
            WHERE ${whereClause}
            ORDER BY search_rank DESC, ${schema.hasCreatedAt ? 'created_at DESC' : validSearchColumns[0]}
            LIMIT ${paramIndex++} OFFSET ${paramIndex}
        `;

        params.push(limit, offset);

        // Check cache if enabled
        if (useCache) {
            const cacheKey = `fts:${tableName}:${Buffer.from(JSON.stringify({ searchTerm, options })).toString('base64').substring(0, 16)}`;
            const cached = await cacheService.get(cacheKey);
            if (cached) {
                debugDb(`Cache hit for full-text search: ${cacheKey}`);
                return cached;
            }
        }

        const result = await query(queryText, params);

        // Cache result if enabled
        if (useCache) {
            const cacheKey = `fts:${tableName}:${Buffer.from(JSON.stringify({ searchTerm, options })).toString('base64').substring(0, 16)}`;
            await cacheService.set(cacheKey, result.rows, cacheTTL, [`table:${tableName}`]);
        }

        logDatabaseOperation('executeFullTextSearch', queryText, params, startTime, result);
        return result.rows;

    } catch (error) {
        logDatabaseOperation('executeFullTextSearch', `FTS ON ${tableName}`, [searchTerm], startTime, null, error);
        throw error;
    }
};

/**
 * Get database statistics and performance metrics
 * @param {Object} options - Statistics options
 * @returns {Promise<Object>} Database statistics
 */
export const getDatabaseStatistics = async (options = {}) => {
    const startTime = Date.now();

    try {
        const {
            includeTables = true,
            includeIndexes = true,
            includeConnections = true,
            includeQueries = true
        } = options;

        const stats = {
            timestamp: new Date(),
            database: {},
            tables: [],
            indexes: [],
            connections: null,
            slowQueries: []
        };

        // Database-level statistics
        const dbStatsQuery = `
            SELECT 
                pg_database_size(current_database()) as database_size,
                (SELECT count(*) FROM pg_stat_user_tables) as table_count,
                (SELECT count(*) FROM pg_stat_user_indexes) as index_count
        `;

        const dbStatsResult = await query(dbStatsQuery);
        stats.database = dbStatsResult.rows[0];

        // Table statistics
        if (includeTables) {
            const tableStatsQuery = `
                SELECT 
                    schemaname,
                    tablename,
                    n_tup_ins as inserts,
                    n_tup_upd as updates,
                    n_tup_del as deletes,
                    n_live_tup as live_tuples,
                    n_dead_tup as dead_tuples,
                    last_vacuum,
                    last_autovacuum,
                    last_analyze,
                    last_autoanalyze
                FROM pg_stat_user_tables
                ORDER BY n_live_tup DESC
                LIMIT 20
            `;

            const tableStatsResult = await query(tableStatsQuery);
            stats.tables = tableStatsResult.rows;
        }

        // Index statistics
        if (includeIndexes) {
            const indexStatsQuery = `
                SELECT 
                    schemaname,
                    tablename,
                    indexname,
                    idx_tup_read as index_reads,
                    idx_tup_fetch as index_fetches,
                    idx_scan as index_scans
                FROM pg_stat_user_indexes
                WHERE idx_scan > 0
                ORDER BY idx_scan DESC
                LIMIT 20
            `;

            const indexStatsResult = await query(indexStatsQuery);
            stats.indexes = indexStatsResult.rows;
        }

        // Connection statistics
        if (includeConnections) {
            const connectionStatsQuery = `
                SELECT 
                    count(*) as total_connections,
                    count(*) FILTER (WHERE state = 'active') as active_connections,
                    count(*) FILTER (WHERE state = 'idle') as idle_connections,
                    count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction
                FROM pg_stat_activity
                WHERE pid != pg_backend_pid()
            `;

            const connectionStatsResult = await query(connectionStatsQuery);
            stats.connections = connectionStatsResult.rows[0];
        }

        // Slow query analysis (if pg_stat_statements is available)
        if (includeQueries) {
            try {
                const slowQueriesQuery = `
                    SELECT 
                        query,
                        calls,
                        total_time,
                        mean_time,
                        rows,
                        100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
                    FROM pg_stat_statements
                    WHERE query NOT LIKE '%pg_stat_statements%'
                    ORDER BY mean_time DESC
                    LIMIT 10
                `;

                const slowQueriesResult = await query(slowQueriesQuery);
                stats.slowQueries = slowQueriesResult.rows;
            } catch (error) {
                // pg_stat_statements extension might not be available
                stats.slowQueries = [];
            }
        }

        logDatabaseOperation('getDatabaseStatistics', 'STATISTICS', [], startTime, { rowCount: 1 });
        return stats;

    } catch (error) {
        logDatabaseOperation('getDatabaseStatistics', 'STATISTICS', [], startTime, null, error);
        throw error;
    }
};

/**
 * Backup table data to JSON format
 * @param {string} tableName - Table name to backup
 * @param {Object} options - Backup options
 * @returns {Promise<Object>} Backup result
 */
export const backupTableData = async (tableName, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            where = {},
            limit = null,
            batchSize = 1000,
            includeSoftDeleted = false
        } = options;

        // Get table schema
        const schema = await getTableSchema(tableName);

        const backup = {
            timestamp: new Date(),
            tableName,
            schema: schema,
            totalRecords: 0,
            data: []
        };

        // Get total count
        backup.totalRecords = await countRecords(tableName, where, { includeSoftDeleted });

        if (backup.totalRecords === 0) {
            logDatabaseOperation('backupTableData', `BACKUP ${tableName}`, [], startTime, { rowCount: 0 });
            return backup;
        }

        // Fetch data in batches
        let offset = 0;
        const effectiveLimit = limit || backup.totalRecords;

        while (offset < effectiveLimit) {
            const batchLimit = Math.min(batchSize, effectiveLimit - offset);

            const batchData = await advancedQuery(tableName, {
                where,
                limit: batchLimit,
                offset,
                includeSoftDeleted,
                orderBy: schema.primaryKey?.[0] || Object.keys(schema.columns)[0]
            });

            backup.data.push(...batchData);
            offset += batchSize;

            if (batchData.length < batchLimit) {
                break; // No more data
            }
        }

        logDatabaseOperation('backupTableData', `BACKUP ${tableName}`, [], startTime, { rowCount: backup.data.length });
        return backup;

    } catch (error) {
        logDatabaseOperation('backupTableData', `BACKUP ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

/**
 * Restore table data from backup
 * @param {string} tableName - Table name to restore
 * @param {Object} backupData - Backup data object
 * @param {Object} options - Restore options
 * @returns {Promise<Object>} Restore result
 */
export const restoreTableData = async (tableName, backupData, options = {}) => {
    const startTime = Date.now();

    try {
        const {
            truncateFirst = false,
            onConflict = 'update',
            batchSize = 1000,
            validateSchema = true
        } = options;

        if (!backupData.data || !Array.isArray(backupData.data)) {
            throw new Error('Invalid backup data format');
        }

        let restoredCount = 0;

        await executeTransaction(async (client) => {
            // Truncate table if requested
            if (truncateFirst) {
                await client.query(`TRUNCATE TABLE ${tableName} RESTART IDENTITY CASCADE`);
            }

            // Get current schema for validation
            let schema = null;
            if (validateSchema) {
                schema = await getTableSchema(tableName);
            }

            // Restore data in batches
            for (let i = 0; i < backupData.data.length; i += batchSize) {
                const batch = backupData.data.slice(i, i + batchSize);

                for (const record of batch) {
                    let validData = record;

                    // Validate against current schema if enabled
                    if (schema) {
                        validData = {};
                        Object.entries(record).forEach(([key, value]) => {
                            if (schema.columns[key]) {
                                validData[key] = value;
                            }
                        });
                    }

                    const result = await insertRecord(tableName, validData, {
                        onConflict,
                        conflictColumns: schema?.primaryKey,
                        validate: false
                    });

                    if (result) restoredCount++;
                }
            }

            return restoredCount;
        });

        // Invalidate cache after restore
        await invalidateCache(tableName, 'restore');

        logDatabaseOperation('restoreTableData', `RESTORE ${tableName}`, [], startTime, { rowCount: restoredCount });
        return {
            tableName,
            restoredRecords: restoredCount,
            totalRecords: backupData.data.length,
            timestamp: new Date()
        };

    } catch (error) {
        logDatabaseOperation('restoreTableData', `RESTORE ${tableName}`, [], startTime, null, error);
        throw error;
    }
};

// Export all functions
export {
    logDatabaseOperation,
    executeWithTimeout,
    invalidateCache,
    generateCacheKeys,
    clearSchemaCache,
    getProcedureSchema
};