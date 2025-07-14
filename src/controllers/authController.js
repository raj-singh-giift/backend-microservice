import { AuthService } from '../services/authService.js';
import { generateRandomToken } from '../utils/crypto.js';
import { updateRecord, insertRecord } from '../utils/database.js';
import { cacheService } from '../services/cacheService.js';
import logger from '../config/logger.js';
import { getRequestId } from '../middleware/requestTracker.js';
import { query } from '../config/database.js';
import debug from 'debug';

const debugAuthController = debug('app:authController');

debugAuthController('Loading authController');

const authService = new AuthService();

const debugAuth = debug('app:auth');

/**
 * Register a new user
 */
export const register = async (req, res) => {
    const { firstName, lastName, email, password, phone, dateOfBirth } = req.body;
    debugAuth('Registering user:', { firstName, lastName, email, password, phone, dateOfBirth });
    try {
        const result = await authService.register({
            firstName,
            lastName,
            email,
            password,
            phone,
            dateOfBirth
        });

        logger.info('User registration successful', {
            userId: result.user.id,
            email,
            requestId: getRequestId()
        });

        res.status(201).json({
            success: true,
            message: 'Registration successful. Please check your email to verify your account.',
            user: result.user
        });
    } catch (error) {
        logger.error('Registration failed:', {
            error: error.message,
            email,
            requestId: getRequestId()
        });

        if (error.message === 'User already exists with this email') {
            return res.status(409).json({
                success: false,
                message: 'User already exists with this email address'
            });
        }

        throw error;
    }
};

/**
 * Login user
 */
export const login = async (req, res) => {
    const { email, password, rememberMe } = req.body;
    debugAuth('Logging in user:', { email, password, rememberMe });
    try {
        const result = await authService.login(email, password, rememberMe);

        // Set refresh token as HTTP-only cookie
        res.cookie('refreshToken', result.tokens.refreshToken, {
            httpOnly: true,
            secure: process.env.NODE_ENV === 'production',
            sameSite: 'strict',
            maxAge: rememberMe ? 30 * 24 * 60 * 60 * 1000 : 7 * 24 * 60 * 60 * 1000
        });

        logger.info('User login successful', {
            userId: result.user.id,
            email,
            requestId: getRequestId()
        });

        res.json({
            success: true,
            message: 'Login successful',
            user: result.user,
            accessToken: result.tokens.accessToken
        });
    } catch (error) {
        logger.warn('Login failed:', {
            error: error.message,
            email,
            requestId: getRequestId()
        });

        res.status(401).json({
            success: false,
            message: error.message
        });
    }
};

/**
 * Logout user
 */
export const logout = async (req, res) => {
    const { refreshToken } = req.body;
    const userId = req.user.id;
    debugAuth('Logging out user:', { userId, refreshToken });
    try {
        await authService.logout(userId, refreshToken);

        // Clear refresh token cookie
        res.clearCookie('refreshToken');

        logger.info('User logout successful', {
            userId,
            requestId: getRequestId()
        });

        res.json({
            success: true,
            message: 'Logout successful'
        });
    } catch (error) {
        logger.error('Logout failed:', {
            error: error.message,
            userId,
            requestId: getRequestId()
        });

        throw error;
    }
};

/**
 * Refresh access token
 */
export const refreshToken = async (req, res) => {
    const { refreshToken } = req.body;
    debugAuth('Refreshing token:', { refreshToken });
    try {
        // Verify refresh token
        const tokenResult = await query(
            'SELECT user_id FROM refresh_tokens WHERE token = $1 AND expires_at > NOW()',
            [refreshToken]
        );

        if (tokenResult.rows.length === 0) {
            return res.status(401).json({
                success: false,
                message: 'Invalid or expired refresh token'
            });
        }

        // Get user data
        const userResult = await query(
            'SELECT id, first_name, last_name, email, role FROM users WHERE id = $1 AND status = $2',
            [tokenResult.rows[0].user_id, 'active']
        );

        if (userResult.rows.length === 0) {
            return res.status(401).json({
                success: false,
                message: 'User not found or inactive'
            });
        }

        const user = userResult.rows[0];
        const newAccessToken = generateToken(user);

        logger.info('Token refresh successful', {
            userId: user.id,
            requestId: getRequestId()
        });

        res.json({
            success: true,
            accessToken: newAccessToken
        });
    } catch (error) {
        logger.error('Token refresh failed:', {
            error: error.message,
            requestId: getRequestId()
        });

        res.status(401).json({
            success: false,
            message: 'Token refresh failed'
        });
    }
};

/**
 * Forgot password
 */
export const forgotPassword = async (req, res) => {
    const { email } = req.body;
    debugAuth('Forgot password:', { email });
    try {
        // Check if user exists
        const userResult = await query(
            'SELECT id FROM users WHERE email = $1 AND status = $2',
            [email, 'active']
        );

        if (userResult.rows.length === 0) {
            // Don't reveal if email exists
            return res.json({
                success: true,
                message: 'If the email exists, a reset link has been sent'
            });
        }

        const userId = userResult.rows[0].id;

        // Generate reset token
        const resetToken = generateRandomToken();
        const expiresAt = new Date(Date.now() + 60 * 60 * 1000); // 1 hour

        // Store reset token
        await insertRecord('password_resets', {
            user_id: userId,
            token: resetToken,
            expires_at: expiresAt,
            created_at: new Date()
        });

        // TODO: Send email with reset link
        // await emailService.sendPasswordResetEmail(email, resetToken);

        logger.info('Password reset requested', {
            userId,
            email,
            requestId: getRequestId()
        });

        res.json({
            success: true,
            message: 'If the email exists, a reset link has been sent'
        });
    } catch (error) {
        logger.error('Forgot password failed:', {
            error: error.message,
            email,
            requestId: getRequestId()
        });

        throw error;
    }
};

/**
 * Reset password
 */
export const resetPassword = async (req, res) => {
    const { token, password } = req.body;
    debugAuth('Resetting password:', { token, password });
    try {
        // Verify reset token
        const tokenResult = await query(
            'SELECT user_id FROM password_resets WHERE token = $1 AND expires_at > NOW() AND used = FALSE',
            [token]
        );

        if (tokenResult.rows.length === 0) {
            return res.status(400).json({
                success: false,
                message: 'Invalid or expired reset token'
            });
        }

        const userId = tokenResult.rows[0].user_id;

        // Hash new password
        const hashedPassword = await hashPassword(password);

        // Update password
        await updateRecord('users',
            { password_hash: hashedPassword, updated_at: new Date() },
            { id: userId }
        );

        // Mark token as used
        await updateRecord('password_resets',
            { used: true, used_at: new Date() },
            { token }
        );

        // Invalidate all refresh tokens for this user
        await query('DELETE FROM refresh_tokens WHERE user_id = $1', [userId]);

        // Clear user cache
        await cacheService.delete(`user:${userId}`);

        logger.info('Password reset successful', {
            userId,
            requestId: getRequestId()
        });

        res.json({
            success: true,
            message: 'Password reset successful'
        });
    } catch (error) {
        logger.error('Password reset failed:', {
            error: error.message,
            requestId: getRequestId()
        });

        throw error;
    }
};

/**
 * Change password (authenticated user)
 */
export const changePassword = async (req, res) => {
    const { currentPassword, newPassword } = req.body;
    const userId = req.user.id;
    debugAuth('Changing password:', { userId, currentPassword, newPassword });
    try {
        // Get current password hash
        const userResult = await query(
            'SELECT password_hash FROM users WHERE id = $1',
            [userId]
        );

        if (userResult.rows.length === 0) {
            return res.status(404).json({
                success: false,
                message: 'User not found'
            });
        }

        // Verify current password
        const isValidPassword = await comparePassword(currentPassword, userResult.rows[0].password_hash);
        if (!isValidPassword) {
            return res.status(400).json({
                success: false,
                message: 'Current password is incorrect'
            });
        }

        // Hash new password
        const hashedPassword = await hashPassword(newPassword);

        // Update password
        await updateRecord('users',
            { password_hash: hashedPassword, updated_at: new Date() },
            { id: userId }
        );

        // Invalidate all refresh tokens except current session
        // TODO: Keep current session active

        logger.info('Password change successful', {
            userId,
            requestId: getRequestId()
        });

        res.json({
            success: true,
            message: 'Password changed successfully'
        });
    } catch (error) {
        logger.error('Password change failed:', {
            error: error.message,
            userId,
            requestId: getRequestId()
        });

        throw error;
    }
};

/**
 * Verify email address
 */
export const verifyEmail = async (req, res) => {
    const { token } = req.params;
    debugAuth('Verifying email:', { token });
    try {
        // Verify email token
        const tokenResult = await query(
            'SELECT user_id FROM email_verifications WHERE token = $1 AND expires_at > NOW()',
            [token]
        );

        if (tokenResult.rows.length === 0) {
            return res.status(400).json({
                success: false,
                message: 'Invalid or expired verification token'
            });
        }

        const userId = tokenResult.rows[0].user_id;

        // Update user email verification status
        await updateRecord('users',
            { email_verified: true, updated_at: new Date() },
            { id: userId }
        );

        // Delete verification token
        await query('DELETE FROM email_verifications WHERE token = $1', [token]);

        logger.info('Email verification successful', {
            userId,
            requestId: getRequestId()
        });

        res.json({
            success: true,
            message: 'Email verified successfully'
        });
    } catch (error) {
        logger.error('Email verification failed:', {
            error: error.message,
            requestId: getRequestId()
        });

        throw error;
    }
};