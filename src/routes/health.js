import { Router } from 'express';
import { asyncHandler } from '../middleware/errorHandler.js';
import {
    healthCheck,
    detailedHealthCheck,
    readinessCheck,
    livenessCheck
} from '../controllers/healthController.js';

const router = Router();

/**
 * @route   GET /api/health
 * @desc    Basic health check
 * @access  Public
 */
router.get('/',
    asyncHandler(healthCheck)
);

/**
 * @route   GET /api/health/detailed
 * @desc    Detailed health check with dependencies
 * @access  Public
 */
router.get('/detailed',
    asyncHandler(detailedHealthCheck)
);

/**
 * @route   GET /api/health/ready
 * @desc    Kubernetes readiness probe
 * @access  Public
 */
router.get('/ready',
    asyncHandler(readinessCheck)
);

/**
 * @route   GET /api/health/live
 * @desc    Kubernetes liveness probe
 * @access  Public
 */
router.get('/live',
    asyncHandler(livenessCheck)
);

export default router;