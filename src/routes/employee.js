import { Router } from 'express';
import { getEmployees } from '../controllers/employeeController.js';
import { asyncHandler } from '../middleware/errorHandler.js';

const router = Router();

router.get('/', asyncHandler(getEmployees));

export default router;