import { paginatedQuery, updateRecord, deleteRecord } from '../utils/database.js';
import { query } from '../config/database.js';
import { cacheService } from '../services/cacheService.js';
import logger from '../config/logger.js';
import { getRequestId } from '../middleware/requestTracker.js';
import debug from 'debug';

const debugUserController = debug('app:userController');

debugUserController('Loading userController');

/**
 * Get current user profile
 */
export const getProfile = async (req, res) => {
    const userId = req.user.id;

    try {
        // Try to get from cache first
        let user = await cacheService.get(`user:${userId}`);

        if (!user) {
            const userResult = await query(
                `SELECT id, first_name, last_name, email, phone, date_of_birth, bio, 
         location, website, preferences, role, status, email_verified, 
         created_at, updated_at, last_login
         FROM users WHERE id = $1`,
                [userId]
            );

            if (userResult.rows.length === 0) {
                return res.status(404).json({
                    success: false,
                    message: 'User not found'
                });
            }

            user = userResult.rows[0];

            // Cache user data
            await cacheService.set(`user:${userId}`, user, 3600);
        }

        res.json({
            success: true,
            user: {
                id: user.id,
                firstName: user.first_name,
                lastName: user.last_name,
                email: user.email,
                phone: user.phone,
                dateOfBirth: user.date_of_birth,
                bio: user.bio,
                location: user.location,
                website: user.website,
                preferences: user.preferences,
                role: user.role,
                status: user.status,
                emailVerified: user.email_verified,
                createdAt: user.created_at,
                updatedAt: user.updated_at,
                lastLogin: user.last_login
            }
        });
    } catch (error) {
        logger.error('Get profile failed:', {
            error: error.message,
            userId,
            requestId: getRequestId()
        });

        throw error;
    }
};

/**
 * Update current user profile
 */
export const updateProfile = async (req, res) => {
    const userId = req.user.id;
    const updateData = req.body;

    try {
        // Convert camelCase to snake_case for database
        const dbUpdateData = {};
        Object.keys(updateData).forEach(key => {
            const dbKey = key.replace(/([A-Z])/g, '_$1').toLowerCase();
            dbUpdateData[dbKey] = updateData[key];
        });

        const updatedUser = await updateRecord('users', dbUpdateData, { id: userId });

        if (!updatedUser) {
            return res.status(404).json({
                success: false,
                message: 'User not found'
            });
        }

        // Clear user cache
        await cacheService.delete(`user:${userId}`);

        logger.info('Profile updated successfully', {
            userId,
            updatedFields: Object.keys(updateData),
            requestId: getRequestId()
        });

        res.json({
            success: true,
            message: 'Profile updated successfully',
            user: {
                id: updatedUser.id,
                firstName: updatedUser.first_name,
                lastName: updatedUser.last_name,
                email: updatedUser.email,
                phone: updatedUser.phone,
                dateOfBirth: updatedUser.date_of_birth,
                bio: updatedUser.bio,
                location: updatedUser.location,
                website: updatedUser.website,
                preferences: updatedUser.preferences
            }
        });
    } catch (error) {
        logger.error('Update profile failed:', {
            error: error.message,
            userId,
            requestId: getRequestId()
        });

        throw error;
    }
};

/**
 * Delete current user account
 */
export const deleteProfile = async (req, res) => {
    const userId = req.user.id;

    try {
        // Soft delete user account
        const deletedUser = await deleteRecord('users', { id: userId }, {
            softDelete: true,
            returning: 'id'
        });

        if (!deletedUser) {
            return res.status(404).json({
                success: false,
                message: 'User not found'
            });
        }

        // Clear all user-related cache
        await cacheService.delete(`user:${userId}`);

        // Delete all refresh tokens
        await query('DELETE FROM refresh_tokens WHERE user_id = $1', [userId]);

        logger.info('User account deleted', {
            userId,
            requestId: getRequestId()
        });

        res.json({
            success: true,
            message: 'Account deleted successfully'
        });
    } catch (error) {
        logger.error('Delete profile failed:', {
            error: error.message,
            userId,
            requestId: getRequestId()
        });

        throw error;
    }
};

/**
 * Get all users (admin only)
 */
export const getUsers = async (req, res) => {
    const { page, limit, search, role, status, sortBy, sortOrder } = req.query;

    try {
        let baseQuery = 'SELECT id, first_name, last_name, email, role, status, email_verified, created_at, updated_at, last_login FROM users WHERE deleted_at IS NULL';
        const params = [];
        let paramIndex = 1;

        // Add search filter
        if (search) {
            baseQuery += ` AND (first_name ILIKE ${paramIndex} OR last_name ILIKE ${paramIndex} OR email ILIKE ${paramIndex})`;
            params.push(`%${search}%`);
            paramIndex++;
        }

        // Add role filter
        if (role) {
            baseQuery += ` AND role = ${paramIndex}`;
            params.push(role);
            paramIndex++;
        }

        // Add status filter
        if (status) {
            baseQuery += ` AND status = ${paramIndex}`;
            params.push(status);
            paramIndex++;
        }

        const result = await paginatedQuery(baseQuery, params, {
            page: parseInt(page),
            limit: parseInt(limit),
            orderBy: sortBy,
            orderDirection: sortOrder.toUpperCase()
        });

        res.json({
            success: true,
            users: result.data.map(user => ({
                id: user.id,
                firstName: user.first_name,
                lastName: user.last_name,
                email: user.email,
                role: user.role,
                status: user.status,
                emailVerified: user.email_verified,
                createdAt: user.created_at,
                updatedAt: user.updated_at,
                lastLogin: user.last_login
            })),
            pagination: result.pagination
        });
    } catch (error) {
        logger.error('Get users failed:', {
            error: error.message,
            requestId: getRequestId()
        });

        throw error;
    }
};

/**
 * Get user by ID (admin only)
 */
export const getUserById = async (req, res) => {
    const { id } = req.params;

    try {
        const userResult = await query(
            `SELECT id, first_name, last_name, email, phone, date_of_birth, bio,
       location, website, preferences, role, status, email_verified,
       created_at, updated_at, last_login
       FROM users WHERE id = $1 AND deleted_at IS NULL`,
            [id]
        );

        if (userResult.rows.length === 0) {
            return res.status(404).json({
                success: false,
                message: 'User not found'
            });
        }

        const user = userResult.rows[0];

        res.json({
            success: true,
            user: {
                id: user.id,
                firstName: user.first_name,
                lastName: user.last_name,
                email: user.email,
                phone: user.phone,
                dateOfBirth: user.date_of_birth,
                bio: user.bio,
                location: user.location,
                website: user.website,
                preferences: user.preferences,
                role: user.role,
                status: user.status,
                emailVerified: user.email_verified,
                createdAt: user.created_at,
                updatedAt: user.updated_at,
                lastLogin: user.last_login
            }
        });
    } catch (error) {
        logger.error('Get user by ID failed:', {
            error: error.message,
            userId: id,
            requestId: getRequestId()
        });

        throw error;
    }
};

/**
 * Update user by ID (admin only)
 */
export const updateUser = async (req, res) => {
    const { id } = req.params;
    const updateData = req.body;

    try {
        // Convert camelCase to snake_case for database
        const dbUpdateData = {};
        Object.keys(updateData).forEach(key => {
            const dbKey = key.replace(/([A-Z])/g, '_$1').toLowerCase();
            dbUpdateData[dbKey] = updateData[key];
        });

        const updatedUser = await updateRecord('users', dbUpdateData, {
            id,
            deleted_at: null
        });

        if (!updatedUser) {
            return res.status(404).json({
                success: false,
                message: 'User not found'
            });
        }

        // Clear user cache
        await cacheService.delete(`user:${id}`);

        logger.info('User updated by admin', {
            targetUserId: id,
            adminUserId: req.user.id,
            updatedFields: Object.keys(updateData),
            requestId: getRequestId()
        });

        res.json({
            success: true,
            message: 'User updated successfully',
            user: {
                id: updatedUser.id,
                firstName: updatedUser.first_name,
                lastName: updatedUser.last_name,
                email: updatedUser.email,
                role: updatedUser.role,
                status: updatedUser.status
            }
        });
    } catch (error) {
        logger.error('Update user failed:', {
            error: error.message,
            targetUserId: id,
            adminUserId: req.user.id,
            requestId: getRequestId()
        });

        throw error;
    }
};

/**
 * Delete user by ID (admin only)
 */
export const deleteUser = async (req, res) => {
    const { id } = req.params;

    try {
        // Prevent admin from deleting themselves
        if (id === req.user.id) {
            return res.status(400).json({
                success: false,
                message: 'Cannot delete your own account'
            });
        }

        const deletedUser = await deleteRecord('users', { id }, {
            softDelete: true,
            returning: 'id'
        });

        if (!deletedUser) {
            return res.status(404).json({
                success: false,
                message: 'User not found'
            });
        }

        // Clear user cache
        await cacheService.delete(`user:${id}`);

        // Delete all refresh tokens for this user
        await query('DELETE FROM refresh_tokens WHERE user_id = $1', [id]);

        logger.info('User deleted by admin', {
            targetUserId: id,
            adminUserId: req.user.id,
            requestId: getRequestId()
        });

        res.json({
            success: true,
            message: 'User deleted successfully'
        });
    } catch (error) {
        logger.error('Delete user failed:', {
            error: error.message,
            targetUserId: id,
            adminUserId: req.user.id,
            requestId: getRequestId()
        });

        throw error;
    }
};