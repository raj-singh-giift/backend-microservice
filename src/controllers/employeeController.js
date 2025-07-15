import debug from 'debug';
import logger from '../config/logger.js';
import {
    findById,
    findWhere,
    insertRecord,
    updateRecord,
    deleteRecord,
    countRecords,
    paginatedQuery,
    getTableSchema,
    verifyTable
} from '../utils/database.js';
import { getRequestId } from '../middleware/requestTracker.js';
import { cacheService } from '../services/cacheService.js';

const debugEmployeeController = debug('app:employeeController');

debugEmployeeController('Loading employeeController');

/**
 * Get employees with pagination and filtering
 */
export const getEmployees = async (req, res) => {
    const requestId = getRequestId();

    try {
        const {
            page = 1,
            limit = 10,
            sortBy = null,
            sortOrder = 'DESC',
            search = null,
            status = null,
            department = null,
            includeSoftDeleted = false
        } = req.query;

        debugEmployeeController('Getting employees with params:', { page, limit, sortBy, sortOrder, search });

        // Verify table exists and get schema
        const tableVerification = await verifyTable('employee');
        if (!tableVerification.exists) {
            return res.status(404).json({
                success: false,
                message: 'Employee table not found',
                requestId
            });
        }

        const schema = tableVerification.schema;

        // Build where conditions based on available columns
        const whereConditions = {};

        if (status && schema.columns.status) {
            whereConditions.status = status;
        }

        if (department && schema.columns.department) {
            whereConditions.department = department;
        }

        // Build search query if search term provided
        let baseQuery = 'SELECT * FROM employee';
        let params = [];
        let paramIndex = 1;

        let whereClauses = [];

        // Add basic where conditions
        Object.entries(whereConditions).forEach(([key, value]) => {
            whereClauses.push(`${key} = $${paramIndex++}`);
            params.push(value);
        });

        // Add search functionality if search term provided and relevant columns exist
        if (search) {
            const searchClauses = [];

            // Search in common employee fields if they exist
            const searchableFields = ['first_name', 'last_name', 'email', 'emp_id', 'name'];
            searchableFields.forEach(field => {
                if (schema.columns[field]) {
                    searchClauses.push(`${field} ILIKE $${paramIndex}`);
                }
            });

            if (searchClauses.length > 0) {
                whereClauses.push(`(${searchClauses.join(' OR ')})`);
                // Add the search parameter for each searchable field
                for (let i = 0; i < searchClauses.length; i++) {
                    params.push(`%${search}%`);
                    paramIndex++;
                }
            }
        }

        // Handle soft delete filtering
        const shouldFilterSoftDeleted = !includeSoftDeleted && schema.hasDeletedAt;
        if (shouldFilterSoftDeleted) {
            whereClauses.push('(deleted_at IS NULL OR deleted_at > NOW())');
        }

        // Build final query
        if (whereClauses.length > 0) {
            baseQuery += ` WHERE ${whereClauses.join(' AND ')}`;
        }

        // Determine sort order
        let orderBy = null;
        if (sortBy && schema.columns[sortBy]) {
            orderBy = `${sortBy} ${sortOrder.toUpperCase()}`;
        } else if (schema.columns.emp_id) {
            orderBy = `emp_id ${sortOrder.toUpperCase()}`;
        } else if (schema.hasCreatedAt) {
            orderBy = `created_at ${sortOrder.toUpperCase()}`;
        }

        // Execute paginated query with caching
        const result = await paginatedQuery(baseQuery, params, {
            page: parseInt(page),
            limit: parseInt(limit),
            orderBy,
            tableName: 'employee',
            useCache: true,
            cacheTTL: 300 // 5 minutes cache
        });

        debugEmployeeController('Retrieved employees:', {
            count: result.data.length,
            pagination: result.pagination
        });

        res.json({
            success: true,
            data: result.data,
            pagination: result.pagination,
            requestId
        });

    } catch (error) {
        logger.error('Error getting employees:', {
            error: error.message,
            stack: error.stack,
            requestId
        });

        res.status(500).json({
            success: false,
            message: 'Failed to retrieve employees',
            requestId
        });
    }
};

/**
 * Get single employee by ID
 */
export const getEmployeeById = async (req, res) => {
    const requestId = getRequestId();
    const { id } = req.params;

    try {
        debugEmployeeController('Getting employee by ID:', id);

        // Get table schema to determine primary key
        const schema = await getTableSchema('employee');

        // Find the primary key column (emp_id, id, or employee_id)
        let primaryKeyColumn = 'id';
        if (schema.columns.emp_id) {
            primaryKeyColumn = 'emp_id';
        } else if (schema.columns.employee_id) {
            primaryKeyColumn = 'employee_id';
        } else if (schema.primaryKey && schema.primaryKey.length > 0) {
            primaryKeyColumn = schema.primaryKey[0];
        }

        const employee = await findById('employee', id, {
            useCache: true,
            cacheTTL: 600 // 10 minutes cache for individual records
        });

        if (!employee) {
            return res.status(404).json({
                success: false,
                message: 'Employee not found',
                requestId
            });
        }

        debugEmployeeController('Retrieved employee:', { id: employee[primaryKeyColumn] });

        res.json({
            success: true,
            data: employee,
            requestId
        });

    } catch (error) {
        logger.error('Error getting employee by ID:', {
            error: error.message,
            id,
            requestId
        });

        res.status(500).json({
            success: false,
            message: 'Failed to retrieve employee',
            requestId
        });
    }
};

/**
 * Create new employee
 */
export const createEmployee = async (req, res) => {
    const requestId = getRequestId();

    try {
        debugEmployeeController('Creating employee with data:', req.body);

        // Validate required fields exist in table schema
        const schema = await getTableSchema('employee');

        // Filter request body to only include valid columns
        const validData = {};
        Object.entries(req.body).forEach(([key, value]) => {
            if (schema.columns[key]) {
                validData[key] = value;
            } else {
                logger.warn(`Column '${key}' not found in employee table, skipping`, { requestId });
            }
        });

        if (Object.keys(validData).length === 0) {
            return res.status(400).json({
                success: false,
                message: 'No valid employee data provided',
                requestId
            });
        }

        const employee = await insertRecord('employee', validData, {
            returning: '*',
            onConflict: 'ignore', // Prevent duplicates if unique constraints exist
            conflictColumns: schema.primaryKey
        });

        if (!employee) {
            return res.status(409).json({
                success: false,
                message: 'Employee with this information already exists',
                requestId
            });
        }

        debugEmployeeController('Created employee:', { id: employee.id || employee.emp_id });

        res.status(201).json({
            success: true,
            message: 'Employee created successfully',
            data: employee,
            requestId
        });

    } catch (error) {
        logger.error('Error creating employee:', {
            error: error.message,
            data: req.body,
            requestId
        });

        res.status(500).json({
            success: false,
            message: 'Failed to create employee',
            requestId
        });
    }
};

/**
 * Update employee
 */
export const updateEmployee = async (req, res) => {
    const requestId = getRequestId();
    const { id } = req.params;

    try {
        debugEmployeeController('Updating employee:', { id, data: req.body });

        // Get table schema
        const schema = await getTableSchema('employee');

        // Determine primary key column
        let primaryKeyColumn = 'id';
        if (schema.columns.emp_id) {
            primaryKeyColumn = 'emp_id';
        } else if (schema.columns.employee_id) {
            primaryKeyColumn = 'employee_id';
        } else if (schema.primaryKey && schema.primaryKey.length > 0) {
            primaryKeyColumn = schema.primaryKey[0];
        }

        // Filter request body to only include valid columns
        const validData = {};
        Object.entries(req.body).forEach(([key, value]) => {
            if (schema.columns[key] && key !== primaryKeyColumn) { // Don't allow updating primary key
                validData[key] = value;
            } else if (key === primaryKeyColumn) {
                logger.warn(`Attempted to update primary key '${key}', skipping`, { requestId });
            } else {
                logger.warn(`Column '${key}' not found in employee table, skipping`, { requestId });
            }
        });

        if (Object.keys(validData).length === 0) {
            return res.status(400).json({
                success: false,
                message: 'No valid update data provided',
                requestId
            });
        }

        const whereConditions = { [primaryKeyColumn]: id };

        const employee = await updateRecord('employee', validData, whereConditions, {
            returning: '*',
            optimisticLocking: schema.hasVersion // Enable optimistic locking if version column exists
        });

        if (!employee) {
            return res.status(404).json({
                success: false,
                message: 'Employee not found or no changes made',
                requestId
            });
        }

        debugEmployeeController('Updated employee:', { id });

        res.json({
            success: true,
            message: 'Employee updated successfully',
            data: employee,
            requestId
        });

    } catch (error) {
        logger.error('Error updating employee:', {
            error: error.message,
            id,
            data: req.body,
            requestId
        });

        res.status(500).json({
            success: false,
            message: 'Failed to update employee',
            requestId
        });
    }
};

/**
 * Delete employee (soft delete if supported, hard delete otherwise)
 */
export const deleteEmployee = async (req, res) => {
    const requestId = getRequestId();
    const { id } = req.params;
    const { force = false } = req.query; // Allow force hard delete

    try {
        debugEmployeeController('Deleting employee:', { id, force });

        // Get table schema
        const schema = await getTableSchema('employee');

        // Determine primary key column
        let primaryKeyColumn = 'id';
        if (schema.columns.emp_id) {
            primaryKeyColumn = 'emp_id';
        } else if (schema.columns.employee_id) {
            primaryKeyColumn = 'employee_id';
        } else if (schema.primaryKey && schema.primaryKey.length > 0) {
            primaryKeyColumn = schema.primaryKey[0];
        }

        const whereConditions = { [primaryKeyColumn]: id };

        // Use soft delete if table supports it and force is not specified
        const useSoftDelete = schema.hasDeletedAt && !force;

        const employee = await deleteRecord('employee', whereConditions, {
            softDelete: useSoftDelete,
            returning: primaryKeyColumn
        });

        if (!employee) {
            return res.status(404).json({
                success: false,
                message: 'Employee not found',
                requestId
            });
        }

        debugEmployeeController('Deleted employee:', { id, softDelete: useSoftDelete });

        res.json({
            success: true,
            message: `Employee ${useSoftDelete ? 'deactivated' : 'deleted'} successfully`,
            data: { id: employee[primaryKeyColumn] },
            requestId
        });

    } catch (error) {
        logger.error('Error deleting employee:', {
            error: error.message,
            id,
            requestId
        });

        res.status(500).json({
            success: false,
            message: 'Failed to delete employee',
            requestId
        });
    }
};

/**
 * Get employee statistics
 */
export const getEmployeeStats = async (req, res) => {
    const requestId = getRequestId();

    try {
        debugEmployeeController('Getting employee statistics');

        // Check cache first
        const cacheKey = 'employee:stats';
        let stats = await cacheService.get(cacheKey);

        if (!stats) {
            const schema = await getTableSchema('employee');

            // Get total count
            const totalCount = await countRecords('employee', {}, {
                includeSoftDeleted: false,
                useCache: true
            });

            // Get deleted count if soft delete is supported
            let deletedCount = 0;
            if (schema.hasDeletedAt) {
                deletedCount = await countRecords('employee', {}, {
                    includeSoftDeleted: true,
                    useCache: true
                }) - totalCount;
            }

            // Get counts by status if status column exists
            let statusCounts = {};
            if (schema.columns.status) {
                const statusResults = await findWhere('employee', {}, {
                    columns: 'status, COUNT(*) as count',
                    useCache: true
                });

                statusCounts = statusResults.reduce((acc, row) => {
                    acc[row.status] = parseInt(row.count);
                    return acc;
                }, {});
            }

            // Get counts by department if department column exists
            let departmentCounts = {};
            if (schema.columns.department) {
                try {
                    // Use GROUP BY query for department counts
                    const departmentQuery = `
                        SELECT department, COUNT(*) as count 
                        FROM employee 
                        WHERE ${schema.hasDeletedAt ? '(deleted_at IS NULL OR deleted_at > NOW())' : '1=1'}
                        GROUP BY department
                    `;
                    const departmentResults = await query(departmentQuery);

                    departmentCounts = departmentResults.rows.reduce((acc, row) => {
                        acc[row.department] = parseInt(row.count);
                        return acc;
                    }, {});
                } catch (error) {
                    logger.warn('Failed to get department counts:', { error: error.message, requestId });
                }
            }

            // Recent activity if created_at exists
            let recentActivity = {};
            if (schema.hasCreatedAt) {
                try {
                    const recentCount = await countRecords('employee', {}, {
                        whereRaw: 'created_at >= NOW() - INTERVAL \'30 days\'',
                        useCache: true
                    });
                    recentActivity.last30Days = recentCount;
                } catch (error) {
                    logger.warn('Failed to get recent activity:', { error: error.message, requestId });
                }
            }

            stats = {
                total: totalCount,
                active: totalCount,
                deleted: deletedCount,
                statusBreakdown: statusCounts,
                departmentBreakdown: departmentCounts,
                recentActivity,
                lastUpdated: new Date().toISOString()
            };

            // Cache stats for 10 minutes
            await cacheService.set(cacheKey, stats, 600, ['table:employee']);
        }

        debugEmployeeController('Retrieved employee statistics:', stats);

        res.json({
            success: true,
            data: stats,
            requestId
        });

    } catch (error) {
        logger.error('Error getting employee statistics:', {
            error: error.message,
            requestId
        });

        res.status(500).json({
            success: false,
            message: 'Failed to retrieve employee statistics',
            requestId
        });
    }
};

/**
 * Search employees with advanced filtering
 */
export const searchEmployees = async (req, res) => {
    const requestId = getRequestId();

    try {
        const {
            q: searchTerm,
            filters = {},
            page = 1,
            limit = 20,
            sortBy = null,
            sortOrder = 'DESC'
        } = req.query;

        debugEmployeeController('Searching employees:', { searchTerm, filters });

        if (!searchTerm || searchTerm.trim().length < 2) {
            return res.status(400).json({
                success: false,
                message: 'Search term must be at least 2 characters long',
                requestId
            });
        }

        const schema = await getTableSchema('employee');

        // Build search query
        let baseQuery = 'SELECT * FROM employee';
        let params = [];
        let paramIndex = 1;
        let whereClauses = [];

        // Add text search across multiple fields
        const searchClauses = [];
        const searchableFields = ['first_name', 'last_name', 'email', 'emp_id', 'name', 'phone'];

        searchableFields.forEach(field => {
            if (schema.columns[field]) {
                searchClauses.push(`${field} ILIKE ${paramIndex}`);
            }
        });

        if (searchClauses.length > 0) {
            whereClauses.push(`(${searchClauses.join(' OR ')})`);
            // Add search parameter for each field
            for (let i = 0; i < searchClauses.length; i++) {
                params.push(`%${searchTerm.trim()}%`);
                paramIndex++;
            }
        }

        // Add filters
        Object.entries(filters).forEach(([key, value]) => {
            if (schema.columns[key] && value !== undefined && value !== '') {
                whereClauses.push(`${key} = ${paramIndex++}`);
                params.push(value);
            }
        });

        // Handle soft delete filtering
        if (schema.hasDeletedAt) {
            whereClauses.push('(deleted_at IS NULL OR deleted_at > NOW())');
        }

        // Build final query
        if (whereClauses.length > 0) {
            baseQuery += ` WHERE ${whereClauses.join(' AND ')}`;
        }

        // Determine sort order
        let orderBy = null;
        if (sortBy && schema.columns[sortBy]) {
            orderBy = `${sortBy} ${sortOrder.toUpperCase()}`;
        } else if (schema.hasCreatedAt) {
            orderBy = `created_at ${sortOrder.toUpperCase()}`;
        }

        const result = await paginatedQuery(baseQuery, params, {
            page: parseInt(page),
            limit: parseInt(limit),
            orderBy,
            tableName: 'employee',
            useCache: false // Don't cache search results as they're dynamic
        });

        debugEmployeeController('Search completed:', {
            searchTerm,
            resultsCount: result.data.length,
            totalMatches: result.pagination.total
        });

        res.json({
            success: true,
            data: result.data,
            pagination: result.pagination,
            searchTerm,
            filters,
            requestId
        });

    } catch (error) {
        logger.error('Error searching employees:', {
            error: error.message,
            searchTerm: req.query.q,
            requestId
        });

        res.status(500).json({
            success: false,
            message: 'Failed to search employees',
            requestId
        });
    }
};

/**
 * Bulk operations on employees
 */
export const bulkEmployeeOperations = async (req, res) => {
    const requestId = getRequestId();

    try {
        const { operation, employees, data = {} } = req.body;

        debugEmployeeController('Bulk operation:', { operation, count: employees?.length });

        if (!operation || !employees || !Array.isArray(employees)) {
            return res.status(400).json({
                success: false,
                message: 'Invalid bulk operation request',
                requestId
            });
        }

        const schema = await getTableSchema('employee');
        let results = [];

        // Determine primary key column
        let primaryKeyColumn = 'id';
        if (schema.columns.emp_id) {
            primaryKeyColumn = 'emp_id';
        } else if (schema.columns.employee_id) {
            primaryKeyColumn = 'employee_id';
        } else if (schema.primaryKey && schema.primaryKey.length > 0) {
            primaryKeyColumn = schema.primaryKey[0];
        }

        switch (operation) {
            case 'update':
                // Filter data to only include valid columns
                const validUpdateData = {};
                Object.entries(data).forEach(([key, value]) => {
                    if (schema.columns[key] && key !== primaryKeyColumn) {
                        validUpdateData[key] = value;
                    }
                });

                if (Object.keys(validUpdateData).length === 0) {
                    return res.status(400).json({
                        success: false,
                        message: 'No valid update data provided',
                        requestId
                    });
                }

                for (const employeeId of employees) {
                    try {
                        const result = await updateRecord('employee', validUpdateData,
                            { [primaryKeyColumn]: employeeId },
                            { returning: primaryKeyColumn }
                        );
                        if (result) {
                            results.push({ id: employeeId, status: 'updated' });
                        }
                    } catch (error) {
                        results.push({ id: employeeId, status: 'failed', error: error.message });
                    }
                }
                break;

            case 'delete':
                const useSoftDelete = schema.hasDeletedAt && !data.force;

                for (const employeeId of employees) {
                    try {
                        const result = await deleteRecord('employee',
                            { [primaryKeyColumn]: employeeId },
                            {
                                softDelete: useSoftDelete,
                                returning: primaryKeyColumn
                            }
                        );
                        if (result) {
                            results.push({
                                id: employeeId,
                                status: useSoftDelete ? 'deactivated' : 'deleted'
                            });
                        }
                    } catch (error) {
                        results.push({ id: employeeId, status: 'failed', error: error.message });
                    }
                }
                break;

            case 'restore':
                if (!schema.hasDeletedAt) {
                    return res.status(400).json({
                        success: false,
                        message: 'Restore operation not supported - table does not support soft deletes',
                        requestId
                    });
                }

                for (const employeeId of employees) {
                    try {
                        const result = await updateRecord('employee',
                            { deleted_at: null },
                            { [primaryKeyColumn]: employeeId },
                            { returning: primaryKeyColumn }
                        );
                        if (result) {
                            results.push({ id: employeeId, status: 'restored' });
                        }
                    } catch (error) {
                        results.push({ id: employeeId, status: 'failed', error: error.message });
                    }
                }
                break;

            default:
                return res.status(400).json({
                    success: false,
                    message: `Unsupported bulk operation: ${operation}`,
                    requestId
                });
        }

        const successCount = results.filter(r => r.status !== 'failed').length;
        const failureCount = results.length - successCount;

        debugEmployeeController('Bulk operation completed:', {
            operation,
            total: results.length,
            success: successCount,
            failed: failureCount
        });

        res.json({
            success: true,
            message: `Bulk ${operation} completed`,
            results,
            summary: {
                total: results.length,
                successful: successCount,
                failed: failureCount
            },
            requestId
        });

    } catch (error) {
        logger.error('Error in bulk employee operation:', {
            error: error.message,
            operation: req.body?.operation,
            requestId
        });

        res.status(500).json({
            success: false,
            message: 'Bulk operation failed',
            requestId
        });
    }
};

/**
 * Get employee table schema information
 */
export const getEmployeeSchema = async (req, res) => {
    const requestId = getRequestId();

    try {
        debugEmployeeController('Getting employee table schema');

        const tableInfo = await verifyTable('employee', { createIndexes: false });

        if (!tableInfo.exists) {
            return res.status(404).json({
                success: false,
                message: 'Employee table not found',
                requestId
            });
        }

        // Transform schema info for frontend consumption
        const schemaInfo = {
            tableName: tableInfo.tableName,
            columns: Object.entries(tableInfo.schema.columns).map(([name, info]) => ({
                name,
                type: info.type,
                nullable: info.nullable,
                maxLength: info.maxLength,
                hasDefault: !!info.default
            })),
            features: {
                softDelete: tableInfo.schema.hasDeletedAt,
                timestamps: {
                    createdAt: tableInfo.schema.hasCreatedAt,
                    updatedAt: tableInfo.schema.hasUpdatedAt
                },
                versioning: tableInfo.schema.hasVersion
            },
            primaryKey: tableInfo.schema.primaryKey,
            indexes: tableInfo.indexes.map(idx => idx.indexname),
            recommendations: tableInfo.recommendations
        };

        res.json({
            success: true,
            data: schemaInfo,
            requestId
        });

    } catch (error) {
        logger.error('Error getting employee schema:', {
            error: error.message,
            requestId
        });

        res.status(500).json({
            success: false,
            message: 'Failed to retrieve schema information',
            requestId
        });
    }
};

export default {
    getEmployees,
    getEmployeeById,
    createEmployee,
    updateEmployee,
    deleteEmployee,
    getEmployeeStats,
    searchEmployees,
    bulkEmployeeOperations,
    getEmployeeSchema
};