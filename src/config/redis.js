import Redis from 'redis';
import config from './index.js';
import logger from './logger.js';

const debugRedis = debug('app:redis');

let redisClient = null;

/**
 * Redis connection configuration
 */
const redisConfig = {
    socket: {
        host: config.redis.host,
        port: config.redis.port,
        reconnectStrategy: (retries) => {
            if (retries > 10) {
                logger.error('Redis reconnection failed after 10 attempts');
                return new Error('Redis reconnection failed');
            }
            return Math.min(retries * 50, 500);
        }
    },
    password: config.redis.password,
    database: config.redis.db,
    name: 'production-backend',
    lazyConnect: true
};

/**
 * Connect to Redis
 */
export const connectRedis = async () => {
    try {
        if (redisClient && redisClient.isOpen) {
            debugRedis('Redis client already connected');
            return redisClient;
        }

        redisClient = Redis.createClient(redisConfig);

        // Setup event listeners
        setupRedisEventListeners();

        // Connect to Redis
        await redisClient.connect();

        // Test connection
        await redisClient.ping();

        logger.info('Redis connected successfully', {
            service: 'redis',
            host: config.redis.host,
            port: config.redis.port,
            database: config.redis.db
        });

        debugRedis('Redis connection established');
        return redisClient;
    } catch (error) {
        logger.error('Redis connection failed:', error);
        throw error;
    }
};

/**
 * Setup Redis event listeners
 */
const setupRedisEventListeners = () => {
    redisClient.on('connect', () => {
        debugRedis('Redis client connecting...');
    });

    redisClient.on('ready', () => {
        logger.info('Redis client ready', { service: 'redis' });
        debugRedis('Redis client ready');
    });

    redisClient.on('error', (error) => {
        logger.error('Redis client error:', error);
    });

    redisClient.on('end', () => {
        logger.info('Redis connection closed', { service: 'redis' });
        debugRedis('Redis connection closed');
    });

    redisClient.on('reconnecting', () => {
        logger.info('Redis client reconnecting...', { service: 'redis' });
        debugRedis('Redis client reconnecting...');
    });
};

/**
 * Get Redis client instance
 */
export const getRedisClient = () => {
    if (!redisClient || !redisClient.isOpen) {
        throw new Error('Redis not connected. Call connectRedis() first.');
    }
    return redisClient;
};

/**
 * Set value in Redis with TTL
 * @param {string} key - Redis key
 * @param {any} value - Value to store
 * @param {number} ttl - Time to live in seconds
 */
export const setCache = async (key, value, ttl = config.redis.ttl) => {
    try {
        const client = getRedisClient();
        const serializedValue = JSON.stringify(value);

        if (ttl > 0) {
            await client.setEx(key, ttl, serializedValue);
        } else {
            await client.set(key, serializedValue);
        }

        debugRedis(`Cache set: ${key}, TTL: ${ttl}s`);
    } catch (error) {
        logger.error('Cache set failed:', { key, error: error.message });
        throw error;
    }
};

/**
 * Get value from Redis
 * @param {string} key - Redis key
 * @returns {Promise<any>} Cached value or null
 */
export const getCache = async (key) => {
    try {
        const client = getRedisClient();
        const value = await client.get(key);

        if (value === null) {
            debugRedis(`Cache miss: ${key}`);
            return null;
        }

        debugRedis(`Cache hit: ${key}`);
        return JSON.parse(value);
    } catch (error) {
        logger.error('Cache get failed:', { key, error: error.message });
        return null; // Return null on error to prevent app crash
    }
};

/**
 * Delete value from Redis
 * @param {string} key - Redis key
 */
export const deleteCache = async (key) => {
    try {
        const client = getRedisClient();
        const result = await client.del(key);
        debugRedis(`Cache deleted: ${key}, existed: ${result > 0}`);
        return result > 0;
    } catch (error) {
        logger.error('Cache delete failed:', { key, error: error.message });
        throw error;
    }
};

/**
 * Check if key exists in Redis
 * @param {string} key - Redis key
 * @returns {Promise<boolean>} True if key exists
 */
export const existsCache = async (key) => {
    try {
        const client = getRedisClient();
        const exists = await client.exists(key);
        debugRedis(`Cache exists check: ${key}, exists: ${exists > 0}`);
        return exists > 0;
    } catch (error) {
        logger.error('Cache exists check failed:', { key, error: error.message });
        return false;
    }
};

/**
 * Set multiple values in Redis
 * @param {Object} keyValuePairs - Object with key-value pairs
 * @param {number} ttl - Time to live in seconds
 */
export const setCacheMultiple = async (keyValuePairs, ttl = config.redis.ttl) => {
    try {
        const client = getRedisClient();
        const multi = client.multi();

        Object.entries(keyValuePairs).forEach(([key, value]) => {
            const serializedValue = JSON.stringify(value);
            if (ttl > 0) {
                multi.setEx(key, ttl, serializedValue);
            } else {
                multi.set(key, serializedValue);
            }
        });

        await multi.exec();
        debugRedis(`Multiple cache set: ${Object.keys(keyValuePairs).length} keys`);
    } catch (error) {
        logger.error('Multiple cache set failed:', error);
        throw error;
    }
};

/**
 * Redis health check
 */
export const redisHealthCheck = async () => {
    try {
        if (!redisClient || !redisClient.isOpen) {
            return {
                status: 'disconnected',
                timestamp: new Date().toISOString()
            };
        }

        const start = Date.now();
        await redisClient.ping();
        const responseTime = Date.now() - start;

        return {
            status: 'healthy',
            timestamp: new Date().toISOString(),
            responseTime: `${responseTime}ms`
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
 * Close Redis connection
 */
export const closeRedis = async () => {
    if (redisClient && redisClient.isOpen) {
        await redisClient.quit();
        redisClient = null;
        logger.info('Redis connection closed');
        debugRedis('Redis connection closed');
    }
};