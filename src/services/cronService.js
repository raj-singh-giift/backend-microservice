import cron from 'node-cron';
import logger from '../config/logger.js';
import config from '../config/index.js';

/**
 * Cron job manager
 */
class CronJobManager {
    constructor () {
        this.jobs = new Map();
    }

    /**
     * Schedule a new cron job
     */
    schedule(name, cronExpression, task, options = {}) {
        const {
            timezone = 'UTC',
            scheduled = true,
            runOnInit = false
        } = options;

        if (this.jobs.has(name)) {
            logger.warn(`Cron job "${name}" already exists. Stopping existing job.`);
            this.stop(name);
        }

        const job = cron.schedule(cronExpression, async () => {
            const startTime = Date.now();
            logger.info(`Cron job "${name}" started`);

            try {
                await task();
                const duration = Date.now() - startTime;
                logger.info(`Cron job "${name}" completed successfully`, { duration: `${duration}ms` });
            } catch (error) {
                const duration = Date.now() - startTime;
                logger.error(`Cron job "${name}" failed:`, { error: error.message, duration: `${duration}ms` });
            }
        }, {
            scheduled,
            timezone
        });

        this.jobs.set(name, {
            job,
            cronExpression,
            options,
            createdAt: new Date()
        });

        logger.info(`Cron job "${name}" scheduled`, { cronExpression, timezone });

        if (runOnInit && scheduled) {
            this.runNow(name);
        }

        return job;
    }

    /**
     * Stop a cron job
     */
    stop(name) {
        const jobData = this.jobs.get(name);
        if (jobData) {
            jobData.job.stop();
            this.jobs.delete(name);
            logger.info(`Cron job "${name}" stopped and removed`);
            return true;
        }
        return false;
    }

    /**
     * Start a stopped cron job
     */
    start(name) {
        const jobData = this.jobs.get(name);
        if (jobData) {
            jobData.job.start();
            logger.info(`Cron job "${name}" started`);
            return true;
        }
        return false;
    }

    /**
     * Run a cron job immediately
     */
    async runNow(name) {
        const jobData = this.jobs.get(name);
        if (jobData) {
            logger.info(`Running cron job "${name}" immediately`);
            await jobData.job.fireOnTick();
            return true;
        }
        return false;
    }

    /**
     * Get all scheduled jobs
     */
    getJobs() {
        const jobs = {};
        this.jobs.forEach((jobData, name) => {
            jobs[name] = {
                cronExpression: jobData.cronExpression,
                options: jobData.options,
                createdAt: jobData.createdAt,
                running: jobData.job.running
            };
        });
        return jobs;
    }

    /**
     * Remove all jobs
     */
    stopAll() {
        this.jobs.forEach((jobData, name) => {
            jobData.job.stop();
        });
        this.jobs.clear();
        logger.info('All cron jobs stopped');
    }
}

// Create global cron manager instance
const cronManager = new CronJobManager();

// Sample cron jobs
if (config.env !== 'test') {
    // Cleanup expired sessions (every hour)
    cronManager.schedule('cleanup-sessions', '0 * * * *', async () => {
        // Implement session cleanup logic
        logger.info('Running session cleanup');
    });

    // Database maintenance (every day at 2 AM)
    cronManager.schedule('db-maintenance', '0 2 * * *', async () => {
        // Implement database maintenance logic
        logger.info('Running database maintenance');
    });

    // Generate reports (every Monday at 9 AM)
    cronManager.schedule('weekly-reports', '0 9 * * 1', async () => {
        // Implement report generation logic
        logger.info('Generating weekly reports');
    });

    // Health check ping (every 5 minutes)
    cronManager.schedule('health-check', '*/5 * * * *', async () => {
        // Implement health check logic
        logger.debug('Running health check');
    });
}

export default cronManager;