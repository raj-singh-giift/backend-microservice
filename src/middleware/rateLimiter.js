import rateLimit from 'express-rate-limit';
import config from '../config/index.js';
import logger from '../config/logger.js';
import { getRedisClient } from '../config/redis.js';

/**
 * Redis store for rate limiting (optional)
 */
class RedisStore {
    constructor () {
        this.prefix = 'rl:';
    }

    async increment(key) {
        try {
            const client = getRedisClient();
            const redisKey = this.prefix + key;

            const multi = client.multi();
            multi.incr(redisKey);
            multi.expire(redisKey, Math.ceil(config.rateLimit.windowMs / 1000));

            const results = await multi.exec();
            const hits = results[0][1];

            return {
                totalHits: hits,
                resetTime: new Date(Date.now() + config.rateLimit.windowMs)
            };
        } catch (error) {
            logger.error('Rate limit store error:', error);
            throw error;
        }
    }

    async decrement(key) {
        try {
            const client = getRedisClient();
            const redisKey = this.prefix + key;
            await client.decr(redisKey);
        } catch (error) {
            logger.error('Rate limit decrement error:', error);
        }
    }

    async resetKey(key) {
        try {
            const client = getRedisClient();
            const redisKey = this.prefix + key;
            await client.del(redisKey);
        } catch (error) {
            logger.error('Rate limit reset error:', error);
        }
    }
}

/**
 * Main rate limiter
 */
export const rateLimiter = rateLimit({
    windowMs: config.rateLimit.windowMs,
    max: config.rateLimit.maxRequests,
    message: {
        error: 'Too many requests',
        message: config.rateLimit.message,
        retryAfter: Math.ceil(config.rateLimit.windowMs / 1000)
    },
    standardHeaders: config.rateLimit.standardHeaders,
    legacyHeaders: config.rateLimit.legacyHeaders,

    // Use Redis store if available
    store: process.env.NODE_ENV === 'production' ? new RedisStore() : undefined,

    // Custom key generator
    keyGenerator: (req) => {
        return req.user?.id || req.ip;
    },

    // Skip function for certain requests
    skip: (req) => {
        // Skip rate limiting for health checks
        return req.path === '/health';
    },

    // Handler for when limit is exceeded
    handler: (req, res) => {
        logger.warn('Rate limit exceeded:', {
            ip: req.ip,
            userAgent: req.get('User-Agent'),
            path: req.path,
            userId: req.user?.id
        });

        res.status(429).json({
            error: 'Too many requests',
            message: config.rateLimit.message,
            retryAfter: Math.ceil(config.rateLimit.windowMs / 1000)
        });
    }
});

/**
 * Strict rate limiter for sensitive endpoints
 */
export const strictRateLimiter = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 5, // 5 requests per window
    message: {
        error: 'Too many requests',
        message: 'Too many requests for this sensitive endpoint',
        retryAfter: 900 // 15 minutes
    },
    standardHeaders: true,
    legacyHeaders: false,

    handler: (req, res) => {
        logger.warn('Strict rate limit exceeded:', {
            ip: req.ip,
            path: req.path,
            userId: req.user?.id
        });

        res.status(429).json({
            error: 'Too many requests',
            message: 'Too many requests for this sensitive endpoint',
            retryAfter: 900
        });
    }
});



