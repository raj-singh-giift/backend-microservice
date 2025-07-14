
import pkg from 'pg';
import debug from 'debug';
import config from './index.js';
import logger from './logger.js';

const { Pool } = pkg;
const debugDb = debug('app:database');

let pool = null;

/**
 * Database connection configuration
 */
const dbConfig = {
    host: config.database.host,
    port: config.database.port,
    database: config.database.name,
    user: config.database.user,
    password: config.database.password,
    ssl: config.database.ssl,
    min: config.database.pool.min,
    max: config.database.pool.max,
    connectionTimeoutMillis: config.database.connectionTimeoutMillis,
    idleTimeoutMillis: config.database.idleTimeoutMillis,
    query_timeout: config.database.query_timeout,
    application_name: config.app.name
};

/**
 * Connect to PostgreSQL database
 */
export const connectDatabase = async () => {
    try {
        if (pool) {
            debugDb('Database pool already exists');
            return pool;
        }

        pool = new Pool(dbConfig);

        // Test connection
        const client = await pool.connect();
        const result = await client.query('SELECT NOW() as current_time, version() as postgres_version');
        client.release();

        logger.info('Database connected successfully', {
            service: 'database',
            serverTime: result.rows[0].current_time,
            version: result.rows[0].postgres_version.split(' ')[0]
        });

        // Setup pool event listeners
        setupPoolEventListeners();

        debugDb('Database connection established');
        return pool;
    } catch (error) {
        logger.error('Database connection failed:', error);
        throw error;
    }
};

/**
 * Setup pool event listeners for monitoring
 */
const setupPoolEventListeners = () => {
    pool.on('connect', (client) => {
        debugDb('New client connected');
        client.query('SET search_path TO public');
    });

    pool.on('acquire', (client) => {
        debugDb('Client acquired from pool');
    });

    pool.on('remove', (client) => {
        debugDb('Client removed from pool');
    });

    pool.on('error', (error, client) => {
        logger.error('Pool client error:', error);
    });
};

/**
 * Get database pool instance
 */
export const getPool = () => {
    if (!pool) {
        throw new Error('Database not connected. Call connectDatabase() first.');
    }
    return pool;
};

/**
 * Execute parameterized query
 * @param {string} text - SQL query text
 * @param {Array} params - Query parameters
 * @param {Object} options - Query options
 * @returns {Promise<Object>} Query result
 */
export const query = async (text, params = [], options = {}) => {
    const start = Date.now();
    const client = options.client || pool;

    try {
        debugDb('Executing query:', { text, params: params.length });

        const result = await client.query(text, params);
        const duration = Date.now() - start;

        debugDb('Query executed successfully', {
            duration: `${duration}ms`,
            rows: result.rowCount
        });

        return result;
    } catch (error) {
        const duration = Date.now() - start;
        logger.error('Query execution failed:', {
            error: error.message,
            query: text,
            duration: `${duration}ms`,
            service: 'database'
        });
        throw error;
    }
};

/**
 * Execute query within a transaction
 * @param {Function} callback - Function containing queries to execute
 * @returns {Promise<any>} Transaction result
 */
export const transaction = async (callback) => {
    const client = await pool.connect();

    try {
        await client.query('BEGIN');
        debugDb('Transaction started');

        const result = await callback(client);

        await client.query('COMMIT');
        debugDb('Transaction committed');

        return result;
    } catch (error) {
        await client.query('ROLLBACK');
        debugDb('Transaction rolled back');
        logger.error('Transaction failed:', error);
        throw error;
    } finally {
        client.release();
    }
};

/**
 * Check database health
 */
export const healthCheck = async () => {
    try {
        const result = await query('SELECT 1 as health_check');
        return {
            status: 'healthy',
            timestamp: new Date().toISOString(),
            responseTime: 'fast'
        };
    } catch (error) {
        return {
            status: 'unhealthy',
            timestamp: new Date().toISOString(),
            error: error.message
        };
    }
};

/**
 * Close database connection
 */
export const closeDatabase = async () => {
    if (pool) {
        await pool.end();
        pool = null;
        logger.info('Database connection closed');
        debugDb('Database connection closed');
    }
};
