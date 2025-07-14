import { hashPassword, comparePassword, generateRandomToken } from '../utils/crypto.js';
import { generateToken, generateRefreshToken } from '../middleware/auth.js';
import { insertRecord, updateRecord } from '../utils/database.js';
import { cacheService } from './cacheService.js';
import logger from '../config/logger.js';

/**
 * Authentication service
 */
export class AuthService {
    /**
     * Register a new user
     */
    async register(userData) {
        try {
            // Check if user already exists
            const existingUser = await query(
                'SELECT id FROM users WHERE email = $1',
                [userData.email]
            );

            if (existingUser.rows.length > 0) {
                throw new Error('User already exists with this email');
            }

            // Hash password
            const hashedPassword = await hashPassword(userData.password);

            // Create user
            const newUser = await insertRecord('users', {
                first_name: userData.firstName,
                last_name: userData.lastName,
                email: userData.email,
                password_hash: hashedPassword,
                phone: userData.phone,
                date_of_birth: userData.dateOfBirth,
                status: 'active',
                role: 'user',
                email_verified: false,
                created_at: new Date(),
                updated_at: new Date()
            });

            // Generate email verification token
            const verificationToken = generateRandomToken();
            await insertRecord('email_verifications', {
                user_id: newUser.id,
                token: verificationToken,
                expires_at: new Date(Date.now() + 24 * 60 * 60 * 1000), // 24 hours
                created_at: new Date()
            });

            logger.info('User registered successfully', { userId: newUser.id, email: userData.email });

            return {
                user: {
                    id: newUser.id,
                    firstName: newUser.first_name,
                    lastName: newUser.last_name,
                    email: newUser.email,
                    role: newUser.role
                },
                verificationToken
            };
        } catch (error) {
            logger.error('User registration failed:', error);
            throw error;
        }
    }

    /**
     * Login user
     */
    async login(email, password, rememberMe = false) {
        try {
            // Get user with password
            const userResult = await query(
                'SELECT id, first_name, last_name, email, password_hash, role, status, email_verified FROM users WHERE email = $1',
                [email]
            );

            if (userResult.rows.length === 0) {
                throw new Error('Invalid credentials');
            }

            const user = userResult.rows[0];

            // Check if user is active
            if (user.status !== 'active') {
                throw new Error('Account is not active');
            }

            // Verify password
            const isValidPassword = await comparePassword(password, user.password_hash);
            if (!isValidPassword) {
                throw new Error('Invalid credentials');
            }

            // Generate tokens
            const accessToken = generateToken(user);
            const refreshToken = generateRefreshToken(user);

            // Store refresh token
            await insertRecord('refresh_tokens', {
                user_id: user.id,
                token: refreshToken,
                expires_at: new Date(Date.now() + (rememberMe ? 30 : 7) * 24 * 60 * 60 * 1000),
                created_at: new Date()
            });

            // Update last login
            await updateRecord('users',
                { last_login: new Date() },
                { id: user.id }
            );

            // Cache user data
            await cacheService.set(`user:${user.id}`, {
                id: user.id,
                firstName: user.first_name,
                lastName: user.last_name,
                email: user.email,
                role: user.role
            }, 3600); // 1 hour

            logger.info('User logged in successfully', { userId: user.id, email });

            return {
                user: {
                    id: user.id,
                    firstName: user.first_name,
                    lastName: user.last_name,
                    email: user.email,
                    role: user.role,
                    emailVerified: user.email_verified
                },
                tokens: {
                    accessToken,
                    refreshToken
                }
            };
        } catch (error) {
            logger.error('User login failed:', error);
            throw error;
        }
    }

    /**
     * Logout user
     */
    async logout(userId, refreshToken) {
        try {
            // Remove refresh token
            await query(
                'DELETE FROM refresh_tokens WHERE user_id = $1 AND token = $2',
                [userId, refreshToken]
            );

            // Clear user cache
            await cacheService.delete(`user:${userId}`);

            logger.info('User logged out successfully', { userId });
        } catch (error) {
            logger.error('User logout failed:', error);
            throw error;
        }
    }
}