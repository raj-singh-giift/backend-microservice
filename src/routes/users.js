import { Router } from 'express';
import { validate } from '../middleware/validation.js';
import { authenticateToken } from '../middleware/auth.js';
import { asyncHandler } from '../middleware/errorHandler.js';
import {
    updateProfileSchema,
    userQuerySchema,
    userIdSchema
} from '../schemas/userSchemas.js';
import {
    getProfile,
    updateProfile,
    deleteProfile,
    getUsers,
    getUserById,
    updateUser,
    deleteUser
} from '../controllers/userController.js';

const router = Router();

/**
 * @route   GET /api/users/profile
 * @desc    Get current user profile
 * @access  Private
 */
router.get('/profile',
    authenticateToken(),
    asyncHandler(getProfile)
);

/**
 * @route   PUT /api/users/profile
 * @desc    Update current user profile
 * @access  Private
 */
router.put('/profile',
    authenticateToken(),
    validate(updateProfileSchema),
    asyncHandler(updateProfile)
);

/**
 * @route   DELETE /api/users/profile
 * @desc    Delete current user account
 * @access  Private
 */
router.delete('/profile',
    authenticateToken(),
    asyncHandler(deleteProfile)
);

/**
 * @route   GET /api/users
 * @desc    Get all users (admin only)
 * @access  Private (Admin)
 */
router.get('/',
    authenticateToken({ roles: ['admin'] }),
    validate(userQuerySchema, 'query'),
    asyncHandler(getUsers)
);

/**
 * @route   GET /api/users/:id
 * @desc    Get user by ID (admin only)
 * @access  Private (Admin)
 */
router.get('/:id',
    authenticateToken({ roles: ['admin'] }),
    validate(userIdSchema, 'params'),
    asyncHandler(getUserById)
);

/**
 * @route   PUT /api/users/:id
 * @desc    Update user by ID (admin only)
 * @access  Private (Admin)
 */
router.put('/:id',
    authenticateToken({ roles: ['admin'] }),
    validate(userIdSchema, 'params'),
    validate(updateProfileSchema),
    asyncHandler(updateUser)
);

/**
 * @route   DELETE /api/users/:id
 * @desc    Delete user by ID (admin only)
 * @access  Private (Admin)
 */
router.delete('/:id',
    authenticateToken({ roles: ['admin'] }),
    validate(userIdSchema, 'params'),
    asyncHandler(deleteUser)
);

export default router;