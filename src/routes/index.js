import { Router } from 'express';
import authRoutes from './auth.js';
import userRoutes from './users.js';
import healthRoutes from './health.js';
import databaseDocs from './databaseDocs.js';
import databaseDocsRoutes from './databaseDocs.js';

const router = Router();

// API Documentation route
router.get('/', (req, res) => {
    res.json({
        message: 'Production Backend API',
        version: '1.0.0',
        endpoints: {
            auth: '/api/auth',
            users: '/api/users',
            health: '/api/health'
        },
        documentation: '/api/docs'
    });
});

// Mount route modules
router.use('/auth', authRoutes);
router.use('/users', userRoutes);
router.use('/health', healthRoutes);
router.use('/database', databaseDocs);
router.use('/docs/database', databaseDocsRoutes);

export default router;