import { healthCheck as dbHealthCheck } from '../config/database.js';
import { redisHealthCheck } from '../config/redis.js';
import config from '../config/index.js';
import logger from '../config/logger.js';
import cronManager from '../services/cronService.js';

/**
 * Basic health check
 */
export const healthCheck = async (req, res) => {
    res.json({
        status: 'OK',
        timestamp: new Date().toISOString(),
        environment: config.env,
        version: config.app.version,
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        requestId: req.requestId
    });
};

/**
 * Detailed health check with dependencies
 */
export const detailedHealthCheck = async (req, res) => {
    const checks = {
        application: {
            status: 'healthy',
            timestamp: new Date().toISOString(),
            uptime: process.uptime(),
            version: config.app.version,
            environment: config.env
        },
        database: await dbHealthCheck(),
        redis: await redisHealthCheck(),
        memory: {
            usage: process.memoryUsage(),
            status: 'healthy'
        },
        cronJobs: {
            jobs: cronManager.getJobs(),
            status: 'healthy'
        }
    };

    // Determine overall health status
    const isHealthy = Object.values(checks).every(check =>
        check.status === 'healthy' || check.status === 'OK'
    );

    const status = isHealthy ? 'healthy' : 'unhealthy';
    const statusCode = isHealthy ? 200 : 503;

    logger.info('Detailed health check performed', {
        status,
        checks: Object.keys(checks),
        requestId: req.requestId
    });

    res.status(statusCode).json({
        status,
        timestamp: new Date().toISOString(),
        checks,
        requestId: req.requestId
    });
};

/**
 * Kubernetes readiness probe
 */
export const readinessCheck = async (req, res) => {
    try {
        // Check if application can serve traffic
        const dbCheck = await dbHealthCheck();
        const redisCheck = await redisHealthCheck();

        const isReady = dbCheck.status === 'healthy' &&
            (redisCheck.status === 'healthy' || redisCheck.status === 'disconnected');

        if (isReady) {
            res.json({
                status: 'ready',
                timestamp: new Date().toISOString()
            });
        } else {
            res.status(503).json({
                status: 'not ready',
                timestamp: new Date().toISOString(),
                database: dbCheck.status,
                redis: redisCheck.status
            });
        }
    } catch (error) {
        logger.error('Readiness check failed:', error);
        res.status(503).json({
            status: 'not ready',
            timestamp: new Date().toISOString(),
            error: error.message
        });
    }
};

/**
 * Kubernetes liveness probe
 */
export const livenessCheck = async (req, res) => {
    // Simple check to verify the application is alive
    res.json({
        status: 'alive',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        memory: process.memoryUsage().heapUsed
    });
};