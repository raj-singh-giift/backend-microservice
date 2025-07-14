import { setCache, getCache, deleteCache, existsCache, setCacheMultiple } from '../config/redis.js';
import logger from '../config/logger.js';
import config from '../config/index.js';

/**
 * Cache service with advanced features
 */
export class CacheService {
    constructor (keyPrefix = 'app') {
        this.keyPrefix = keyPrefix;
        this.defaultTTL = config.redis.ttl;
    }

    /**
     * Generate cache key with prefix
     */
    generateKey(key) {
        return `${this.keyPrefix}:${key}`;
    }

    /**
     * Set cache with tags for grouping
     */
    async set(key, value, ttl = this.defaultTTL, tags = []) {
        const cacheKey = this.generateKey(key);

        try {
            await setCache(cacheKey, value, ttl);

            // Store tags for cache invalidation
            if (tags.length > 0) {
                const tagKeys = {};
                tags.forEach(tag => {
                    tagKeys[this.generateKey(`tag:${tag}`)] = [cacheKey];
                });
                await setCacheMultiple(tagKeys, ttl);
            }

            logger.debug('Cache set successfully', { key: cacheKey, ttl, tags });
        } catch (error) {
            logger.error('Cache set failed:', error);
            throw error;
        }
    }

    /**
     * Get from cache
     */
    async get(key) {
        const cacheKey = this.generateKey(key);

        try {
            const value = await getCache(cacheKey);
            logger.debug('Cache get', { key: cacheKey, hit: value !== null });
            return value;
        } catch (error) {
            logger.error('Cache get failed:', error);
            return null;
        }
    }

    /**
     * Delete from cache
     */
    async delete(key) {
        const cacheKey = this.generateKey(key);

        try {
            const result = await deleteCache(cacheKey);
            logger.debug('Cache delete', { key: cacheKey, existed: result });
            return result;
        } catch (error) {
            logger.error('Cache delete failed:', error);
            return false;
        }
    }

    /**
     * Check if key exists
     */
    async exists(key) {
        const cacheKey = this.generateKey(key);
        return await existsCache(cacheKey);
    }

    /**
     * Cache with function execution (memoization)
     */
    async remember(key, fn, ttl = this.defaultTTL) {
        const cached = await this.get(key);

        if (cached !== null) {
            logger.debug('Cache hit for remember function', { key });
            return cached;
        }

        logger.debug('Cache miss for remember function, executing', { key });
        const result = await fn();

        await this.set(key, result, ttl);
        return result;
    }

    /**
     * Invalidate cache by tags
     */
    async invalidateByTags(tags) {
        try {
            for (const tag of tags) {
                const tagKey = this.generateKey(`tag:${tag}`);
                const cachedKeys = await getCache(tagKey);

                if (cachedKeys && Array.isArray(cachedKeys)) {
                    for (const key of cachedKeys) {
                        await deleteCache(key);
                    }
                    await deleteCache(tagKey);
                }
            }

            logger.info('Cache invalidated by tags', { tags });
        } catch (error) {
            logger.error('Cache invalidation by tags failed:', error);
        }
    }
}

// Create default cache service instance
export const cacheService = new CacheService();