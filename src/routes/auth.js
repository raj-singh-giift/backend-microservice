import { Router } from 'express';
import { validate } from '../middleware/validation.js';
import { authenticateToken, strictRateLimiter } from '../middleware/index.js';
import { asyncHandler } from '../middleware/errorHandler.js';
import {
    registerSchema,
    loginSchema,
    forgotPasswordSchema,
    resetPasswordSchema,
    changePasswordSchema,
    refreshTokenSchema
} from '../schemas/authSchemas.js';
import {
    register,
    login,
    logout,
    refreshToken,
    forgotPassword,
    resetPassword,
    changePassword,
    verifyEmail
} from '../controllers/authController.js';
import debug from 'debug';

const router = Router();

const debugAuthRoutes = debug('app:authRoutes');

debugAuthRoutes('Loading authRoutes');

/**
 * @route   POST /api/auth/register
 * @desc    Register a new user
 * @access  Public
 */
router.post('/register',
    strictRateLimiter,
    validate(registerSchema),
    asyncHandler(register)
);

/**
 * @route   POST /api/auth/login
 * @desc    Login user
 * @access  Public
 */
router.post('/login',
    strictRateLimiter,
    validate(loginSchema),
    asyncHandler(login)
);

/**
 * @route   POST /api/auth/logout
 * @desc    Logout user
 * @access  Private
 */
router.post('/logout',
    authenticateToken(),
    validate(refreshTokenSchema),
    asyncHandler(logout)
);

/**
 * @route   POST /api/auth/refresh
 * @desc    Refresh access token
 * @access  Public
 */
router.post('/refresh',
    validate(refreshTokenSchema),
    asyncHandler(refreshToken)
);

/**
 * @route   POST /api/auth/forgot-password
 * @desc    Request password reset
 * @access  Public
 */
router.post('/forgot-password',
    strictRateLimiter,
    validate(forgotPasswordSchema),
    asyncHandler(forgotPassword)
);

/**
 * @route   POST /api/auth/reset-password
 * @desc    Reset password with token
 * @access  Public
 */
router.post('/reset-password',
    strictRateLimiter,
    validate(resetPasswordSchema),
    asyncHandler(resetPassword)
);

/**
 * @route   POST /api/auth/change-password
 * @desc    Change password (authenticated user)
 * @access  Private
 */
router.post('/change-password',
    authenticateToken(),
    validate(changePasswordSchema),
    asyncHandler(changePassword)
);

/**
 * @route   GET /api/auth/verify-email/:token
 * @desc    Verify email address
 * @access  Public
 */
router.get('/verify-email/:token',
    asyncHandler(verifyEmail)
);

/**
 * @route   GET /api/auth/me
 * @desc    Get current user info
 * @access  Private
 */
router.get('/me',
    authenticateToken(),
    asyncHandler(async (req, res) => {
        res.json({
            success: true,
            user: req.user
        });
    })
);

export default router;
