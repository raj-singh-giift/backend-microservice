
import { query, transaction } from '../config/database.js';
import logger from '../config/logger.js';
import { getRequestId } from '../middleware/requestTracker.js';

/**
 * Generic paginated query function
 * @param {string} baseQuery - Base SQL query without LIMIT/OFFSET
 * @param {Array} params - Query parameters
 * @param {Object} paginationOptions - Pagination options
 * @returns {Promise<Object>} Paginated results
 */
export const paginatedQuery = async (baseQuery, params = [], paginationOptions = {}) => {
    const {
        page = 1,
        limit = 10,
        orderBy = 'created_at',
        orderDirection = 'DESC'
    } = paginationOptions;

    const offset = (page - 1) * limit;

    // Count total records
    const countQuery = `SELECT COUNT(*) as total FROM (${baseQuery}) as count_query`;
    const countResult = await query(countQuery, params);
    const total = parseInt(countResult.rows[0].total);

    // Get paginated data
    const dataQuery = `
    ${baseQuery}
    ORDER BY ${orderBy} ${orderDirection}
    LIMIT $${params.length + 1} OFFSET $${params.length + 2}
  `;

    const dataResult = await query(dataQuery, [...params, limit, offset]);

    return {
        data: dataResult.rows,
        pagination: {
            page: parseInt(page),
            limit: parseInt(limit),
            total,
            totalPages: Math.ceil(total / limit),
            hasNextPage: page < Math.ceil(total / limit),
            hasPrevPage: page > 1
        }
    };
};

/**
 * Generic insert function with conflict handling
 * @param {string} tableName - Table name
 * @param {Object} data - Data to insert
 * @param {Object} options - Insert options
 * @returns {Promise<Object>} Insert result
 */
export const insertRecord = async (tableName, data, options = {}) => {
    const {
        onConflict = null, // 'ignore' or 'update'
        conflictColumns = ['id'],
        returning = '*'
    } = options;

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

        queryText += ` ON CONFLICT (${conflictColumns.join(', ')}) DO UPDATE SET ${updateSet}`;
    }

    queryText += ` RETURNING ${returning}`;

    const result = await query(queryText, values);
    return result.rows[0];
};

/**
 * Generic update function
 * @param {string} tableName - Table name
 * @param {Object} data - Data to update
 * @param {Object} whereConditions - Where conditions
 * @param {Object} options - Update options
 * @returns {Promise<Object>} Update result
 */
export const updateRecord = async (tableName, data, whereConditions, options = {}) => {
    const { returning = '*' } = options;

    const dataColumns = Object.keys(data);
    const dataValues = Object.values(data);

    const whereColumns = Object.keys(whereConditions);
    const whereValues = Object.values(whereConditions);

    const setClause = dataColumns.map((col, index) => `${col} = $${index + 1}`).join(', ');
    const whereClause = whereColumns.map((col, index) => `${col} = $${dataValues.length + index + 1}`).join(' AND ');

    const queryText = `
    UPDATE ${tableName}
    SET ${setClause}, updated_at = NOW()
    WHERE ${whereClause}
    RETURNING ${returning}
  `;

    const result = await query(queryText, [...dataValues, ...whereValues]);
    return result.rows[0];
};

/**
 * Generic delete function
 * @param {string} tableName - Table name
 * @param {Object} whereConditions - Where conditions
 * @param {Object} options - Delete options
 * @returns {Promise<Object>} Delete result
 */
export const deleteRecord = async (tableName, whereConditions, options = {}) => {
    const { returning = 'id', softDelete = false } = options;

    const whereColumns = Object.keys(whereConditions);
    const whereValues = Object.values(whereConditions);
    const whereClause = whereColumns.map((col, index) => `${col} = $${index + 1}`).join(' AND ');

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
    return result.rows[0];
};

/**
 * Execute stored procedure
 * @param {string} procedureName - Stored procedure name
 * @param {Array} params - Procedure parameters
 * @returns {Promise<Object>} Procedure result
 */
export const executeStoredProcedure = async (procedureName, params = []) => {
    const placeholders = params.map((_, index) => `$${index + 1}`).join(', ');
    const queryText = `CALL ${procedureName}(${placeholders})`;

    return await query(queryText, params);
};

/**
 * Execute function (for PostgreSQL functions that return values)
 * @param {string} functionName - Function name
 * @param {Array} params - Function parameters
 * @returns {Promise<Object>} Function result
 */
export const executeFunction = async (functionName, params = []) => {
    const placeholders = params.map((_, index) => `$${index + 1}`).join(', ');
    const queryText = `SELECT * FROM ${functionName}(${placeholders})`;

    return await query(queryText, params);
};