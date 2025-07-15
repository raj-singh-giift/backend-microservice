import { Router } from 'express';
import authRoutes from './auth.js';
import userRoutes from './users.js';
import healthRoutes from './health.js';
import databaseDocs from './databaseDocs.js';
import employeeRoutes from './employee.js';

const router = Router();

// API Documentation route
router.get('/', (req, res) => {
    res.json({
        message: 'Production Backend API',
        version: '1.0.0',
        endpoints: {
            auth: '/api/auth',
            users: '/api/users',
            health: '/api/health',
            database: '/api/database',
            employees: '/api/employees'
        },
        documentation: '/api/docs'
    });
});

// Mount route modules
router.use('/auth', authRoutes);
router.use('/users', userRoutes);
router.use('/health', healthRoutes);
router.use('/database', databaseDocs);
router.use('/employee', employeeRoutes);

export default router;