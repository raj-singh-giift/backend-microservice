# Database Utilities Documentation

A comprehensive, production-ready database abstraction layer with advanced querying capabilities, timeout support, and extensive logging.

## 🚀 Quick Start

```javascript
import {
    findById,
    findWhere,
    insertRecord,
    updateRecord,
    deleteRecord,
    paginatedQuery,
    groupByQuery,
    advancedQuery
} from '../utils/database.js';
```

## 📚 Table of Contents

1. [Basic CRUD Operations](#basic-crud-operations)
2. [Advanced Queries](#advanced-queries)
3. [Pattern Matching](#pattern-matching)
4. [Aggregations & Analytics](#aggregations--analytics)
5. [Bulk Operations](#bulk-operations)
6. [Transactions](#transactions)
7. [Timeout Configuration](#timeout-configuration)
8. [Error Handling](#error-handling)
9. [Performance Monitoring](#performance-monitoring)
10. [Best Practices](#best-practices)

---

## Basic CRUD Operations

### Find by ID
```javascript
// Basic find
const user = await findById('users', userId);

// With options
const user = await findById('users', userId, {
    columns: 'id, name, email, created_at',
    includeSoftDeleted: false
});
```

### Find with conditions
```javascript
// Simple conditions
const activeUsers = await findWhere('users', 
    { status: 'active', role: 'user' },
    {
        orderBy: 'created_at DESC',
        limit: 50
    }
);

// Multiple values (IN clause)
const specificUsers = await findWhere('users', {
    id: [1, 2, 3, 4, 5]
});
```

### Insert Record
```javascript
// Basic insert
const newUser = await insertRecord('users', {
    name: 'John Doe',
    email: 'john@example.com',
    role: 'user'
});

// Insert with conflict handling
const user = await insertRecord('users', userData, {
    onConflict: 'update',
    conflictColumns: ['email'],
    returning: 'id, name, email'
});

// Insert or ignore
const user = await insertRecord('users', userData, {
    onConflict: 'ignore',
    conflictColumns: ['email']
});
```

### Update Record
```javascript
// Basic update
const updatedUser = await updateRecord('users', 
    { name: 'Jane Doe', updated_at: new Date() },
    { id: userId }
);

// Update with optimistic locking
const updatedUser = await updateRecord('users', 
    { name: 'Jane Doe', version: 2 },
    { id: userId, version: 1 },
    { optimisticLocking: true, versionColumn: 'version' }
);
```

### Delete Record
```javascript
// Soft delete
const deletedUser = await deleteRecord('users', { id: userId }, {
    softDelete: true
});

// Hard delete with cascade
const deletedUser = await deleteRecord('users', { id: userId }, {
    softDelete: false,
    cascadeDelete: [
        { table: 'user_sessions', foreignKey: 'user_id' },
        { table: 'user_preferences', foreignKey: 'user_id' }
    ]
});
```

### Upsert (Insert or Update)
```javascript
const user = await upsertRecord('users', 
    {
        email: 'john@example.com',
        name: 'John Doe',
        last_login: new Date()
    },
    ['email'], // Conflict columns
    {
        excludeFromUpdate: ['created_at'] // Don't update these fields
    }
);
```

---

## Advanced Queries

### Advanced Query Builder
```javascript
// Complex multi-table query
const orders = await advancedQuery('orders', {
    select: `
        orders.*,\n        users.name as customer_name,\n        products.name as product_name,\n        COUNT(order_items.id) as item_count
    `,
    joins: [
        { type: 'LEFT', table: 'users', on: 'users.id = orders.customer_id' },
        { type: 'INNER', table: 'order_items', on: 'order_items.order_id = orders.id' },
        { type: 'INNER', table: 'products', on: 'products.id = order_items.product_id' }
    ],
    where: { 'orders.status': 'completed' },
    whereRaw: 'orders.amount > 100 AND users.tier = \'premium\'',
    groupBy: 'orders.id, users.name, products.name',
    having: 'COUNT(order_items.id) > 1',
    orderBy: 'orders.created_at DESC',
    limit: 100,
    distinct: true,
    timeout: 45000
});
```

### Pagination
```javascript
// Basic pagination
const result = await paginatedQuery(
    'SELECT * FROM users WHERE status = $1',
    ['active'],
    {
        page: 1,
        limit: 20,
        orderBy: 'created_at',
        orderDirection: 'DESC'
    }
);

console.log(result.data); // Array of records
console.log(result.pagination); // Pagination metadata
/*
{
    page: 1,
    limit: 20,
    total: 150,
    totalPages: 8,
    hasNextPage: true,
    hasPrevPage: false,
    offset: 0
}
*/

// High-performance pagination (skip count for better performance)
const result = await paginatedQuery(baseQuery, params, {
    page: 5,
    limit: 50,
    includeCount: false, // Skip total count for performance
    maxLimit: 100
});
```

---

## Pattern Matching

### Text Search and Pattern Matching
```javascript
// Various pattern types
const searchResults = await patternMatch('products', {
    name: { type: 'contains', value: 'laptop' },
    sku: { type: 'regex', value: '^PRD-[0-9]{6}$' },
    description: { type: 'fulltext', value: 'gaming mechanical keyboard' },
    category: { type: 'startsWith', value: 'Electronics' }
}, {
    caseSensitive: false,
    orderBy: 'created_at DESC',
    limit: 50,
    timeout: 30000
});

// Email validation with regex
const validEmails = await patternMatch('users', {
    email: { 
        type: 'regex', 
        value: '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$' 
    }
}, { caseSensitive: true });

// Full-text search
const blogPosts = await patternMatch('posts', {
    content: { type: 'fulltext', value: 'javascript nodejs express' },
    title: { type: 'like', value: '%tutorial%' }
});
```

---

## Aggregations & Analytics

### Group By Queries
```javascript
// User statistics by role
const userStats = await groupByQuery('users', {
    groupBy: 'role',
    aggregates: {
        total_users: 'COUNT(*)',
        avg_age: 'AVG(age)',
        latest_signup: 'MAX(created_at)',
        active_users: { func: 'COUNT', column: '*', filter: 'status = \'active\'' }
    },
    having: 'COUNT(*) > 5',
    orderBy: 'total_users DESC'
});

// Daily sales report
const dailySales = await groupByQuery('orders', {
    groupBy: 'DATE(created_at)',
    select: ['DATE(created_at) as sale_date'],
    aggregates: {
        daily_revenue: 'SUM(amount)',
        order_count: 'COUNT(*)',
        avg_order_value: 'AVG(amount)',
        unique_customers: { func: 'COUNT', column: 'customer_id', distinct: true }
    },
    where: { status: 'completed' },
    orderBy: 'sale_date DESC'
});
```

### Complex Aggregations
```javascript
// Comprehensive analytics
const analytics = await aggregateQuery('orders', {
    aggregates: {
        total_revenue: 'SUM(amount)',
        avg_order: 'AVG(amount)',
        order_count: 'COUNT(*)',
        max_order: 'MAX(amount)',
        min_order: 'MIN(amount)',
        revenue_stddev: 'STDDEV(amount)',
        unique_customers: { func: 'COUNT', column: 'customer_id', distinct: true },
        high_value_orders: { 
            func: 'COUNT', 
            column: '*', 
            filter: 'amount > 1000' 
        },
        refunded_orders: {
            func: 'COUNT',
            column: '*',
            filter: 'status = \'refunded\''
        }
    },
    where: { created_at: { $gte: '2024-01-01' } }
});
```

### Window Functions
```javascript
// Sales ranking and analytics
const salesAnalytics = await windowQuery('sales', {
    select: 'salesperson, amount, sale_date',
    windowFunctions: [
        {
            func: 'ROW_NUMBER',
            alias: 'sale_rank',
            partitionBy: 'salesperson',
            orderBy: 'amount DESC'
        },
        {
            func: 'LAG',
            column: 'amount',
            alias: 'previous_sale',
            partitionBy: 'salesperson',
            orderBy: 'sale_date'
        },
        {
            func: 'SUM',
            column: 'amount',
            alias: 'running_total',
            partitionBy: 'salesperson',
            orderBy: 'sale_date',
            frameClause: 'ROWS UNBOUNDED PRECEDING'
        },
        {
            func: 'PERCENT_RANK',
            alias: 'percentile_rank',
            orderBy: 'amount DESC'
        }
    ],
    orderBy: 'salesperson, sale_rank'
});
```

### Top N Queries
```javascript
// Top 10 customers by revenue
const topCustomers = await getTopRecords('customers', {
    limit: 10,
    orderBy: 'total_revenue DESC',
    where: { status: 'active' }
});

// Top 5 products with ties
const topProducts = await getTopRecords('products', {
    limit: 5,
    orderBy: 'sales_count DESC',
    withTies: true, // Include ties using RANK()
    where: { category: 'Electronics' }
});
```

---

## Bulk Operations

### Bulk Insert
```javascript
// Large dataset insert
const results = await bulkInsert('products', productsArray, {
    batchSize: 1000,
    onConflict: 'update',
    conflictColumns: ['sku'],
    returning: 'id, sku',
    timeout: 300000 // 5 minutes for large datasets
});

// Import with conflict resolution
const importResults = await bulkInsert('users', csvData, {
    batchSize: 500,
    onConflict: 'ignore', // Skip duplicates
    conflictColumns: ['email']
});
```

---

## Transactions

### Basic Transactions
```javascript
// Simple transaction
const result = await executeTransaction(async (client) => {
    const user = await client.query(
        'UPDATE users SET credits = credits - $1 WHERE id = $2 RETURNING credits',
        [amount, userId]
    );
    
    await client.query(
        'INSERT INTO transactions (user_id, amount, type) VALUES ($1, $2, $3)',
        [userId, amount, 'debit']
    );
    
    return user.rows[0];
});

// Transaction with isolation level
const result = await executeTransaction(async (client) => {
    // Critical operations requiring consistency
    const inventory = await client.query(
        'SELECT quantity FROM products WHERE id = $1 FOR UPDATE',
        [productId]
    );
    
    if (inventory.rows[0].quantity < orderQuantity) {
        throw new Error('Insufficient inventory');
    }
    
    await client.query(
        'UPDATE products SET quantity = quantity - $1 WHERE id = $2',
        [orderQuantity, productId]
    );
    
    return await client.query(
        'INSERT INTO orders (product_id, quantity) VALUES ($1, $2) RETURNING *',
        [productId, orderQuantity]
    );
}, {
    timeout: 30000,
    isolationLevel: 'SERIALIZABLE'
});
```

---

## Timeout Configuration

### Default Timeouts
```javascript
// Operation-specific timeouts
const config = {
    default: 30000,        // 30 seconds
    bulk: 60000,          // 1 minute
    analytics: 120000,    // 2 minutes
    transactions: 60000,  // 1 minute
    heavy: 300000        // 5 minutes
};
```

### Custom Timeouts
```javascript
// Short timeout for real-time queries
const user = await findById('users', userId, { timeout: 5000 });

// Long timeout for heavy analytics
const report = await aggregateQuery('orders', {
    aggregates: { /* complex aggregations */ },
    timeout: 180000 // 3 minutes
});

// Per-environment timeouts
const timeout = process.env.NODE_ENV === 'production' ? 30000 : 60000;
const result = await advancedQuery('table', { timeout });
```

### Retry with Exponential Backoff
```javascript
import { executeWithRetry } from '../utils/database.js';

const result = await executeWithRetry(async ({ timeout }) => {
    return await executeStoredProcedure('unreliable_proc', [], { timeout });
}, {
    maxRetries: 3,
    baseTimeout: 30000,
    backoffMultiplier: 2,
    retryOn: ['timeout', 'connection']
});
```

---

## Error Handling

### Timeout Errors
```javascript
try {
    const result = await executeStoredProcedure('long_proc', [], { timeout: 10000 });
} catch (error) {
    if (error.message.includes('timeout')) {
        logger.warn('Query timed out, implementing fallback');
        // Implement fallback strategy
        return await getCachedResult();
    }
    throw error;
}
```

### Validation Errors
```javascript
try {
    await insertRecord('users', invalidData);
} catch (error) {
    if (error.code === '23505') { // Unique violation
        throw new Error('Email already exists');
    }
    if (error.code === '23503') { // Foreign key violation
        throw new Error('Invalid reference');
    }
    throw error;
}
```

---

## Performance Monitoring

### Query Performance Tracking
```javascript
// Automatic performance logging
const result = await paginatedQuery(
    'SELECT * FROM large_table WHERE complex_condition = $1',
    [value],
    { timeout: 60000 }
);
// Logs: "Query executed successfully: duration: 2.5s, rows: 1500"

// Manual performance monitoring
const startTime = Date.now();
const result = await advancedQuery('table', options);
const duration = Date.now() - startTime;

if (duration > 5000) {
    logger.warn('Slow query detected', { duration, query: 'advancedQuery' });
}
```

### Health Checks
```javascript
// Database health monitoring
const health = await databaseHealthCheck();
console.log(health);
/*
{
    status: 'healthy',
    serverTime: '2024-01-15T10:30:00.000Z',
    serverVersion: 'PostgreSQL 15.2',
    responseTime: 45
}
*/

// Table statistics
const stats = await getTableStats('users');
console.log(stats); // Column statistics and distribution
```

---

## JSON Operations

### JSONB Queries
```javascript
// Complex JSON operations
const results = await jsonQuery('users', {
    preferences: [
        { type: 'contains', value: { theme: 'dark', language: 'en' } },
        { type: 'hasKey', path: 'notifications' },
        { type: 'pathValue', path: 'settings.timezone', value: 'UTC', operator: '=' }
    ],
    metadata: [
        { type: 'hasAnyKey', value: ['tag1', 'tag2', 'tag3'] }
    ]
}, {
    orderBy: 'created_at DESC',
    limit: 100
});

// JSON path queries
const users = await jsonQuery('users', {
    profile: [
        { type: 'pathExists', path: 'social.twitter' },
        { type: 'pathValue', path: 'settings.notifications.email', value: 'true' }
    ]
});
```

---

## Range Queries

### Date and Numeric Ranges
```javascript
// Date range queries
const recentOrders = await rangeQuery('orders', {
    created_at: { 
        min: '2024-01-01', 
        max: '2024-12-31' 
    },
    amount: { min: 100, max: 5000 }
}, {
    orderBy: 'created_at DESC',
    includeBounds: [true, false] // Include min, exclude max
});

// Time interval queries
const lastWeekData = await rangeQuery('analytics', {
    timestamp: { 
        interval: '7 days',
        type: 'date'
    }
});

// Numeric ranges with custom bounds
const products = await rangeQuery('products', {
    price: { min: 50, max: 200 },
    rating: { min: 4.0 }
}, {
    includeBounds: [true, true] // Include both bounds
});
```

---

## Subqueries and EXISTS

### EXISTS Queries
```javascript
// Users who have placed orders
const activeCustomers = await existsQuery('users', {
    table: 'orders',
    where: { status: 'completed' },
    correlation: 'orders.customer_id = users.id'
}, {
    exists: true,
    orderBy: 'created_at DESC'
});

// Users without any orders
const prospectCustomers = await existsQuery('users', {
    table: 'orders',
    where: {},
    correlation: 'orders.customer_id = users.id'
}, {
    exists: false,
    select: 'id, name, email, created_at'
});
```

---

## Best Practices

### 1. **Always Use Timeouts**
```javascript
// Good: Specify appropriate timeouts
const result = await advancedQuery('table', { 
    timeout: 30000 
});

// Bad: No timeout (can hang indefinitely)
const result = await advancedQuery('table', {});
```

### 2. **Use Pagination for Large Datasets**
```javascript
// Good: Paginated results
const result = await paginatedQuery(query, params, { 
    limit: 50,
    page: 1 
});

// Bad: Loading all records
const result = await executeRawQuery('SELECT * FROM large_table');
```

### 3. **Leverage Indexes for Pattern Matching**
```javascript
// Good: Use indexed columns for pattern matching
const users = await patternMatch('users', {
    email: { type: 'startsWith', value: 'john' } // email is indexed
});

// Consider: Full-text search for text content
const posts = await patternMatch('posts', {
    content: { type: 'fulltext', value: 'javascript tutorial' }
});
```

### 4. **Use Transactions for Related Operations**
```javascript
// Good: Atomic operations
await executeTransaction(async (client) => {
    await client.query('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [amount, fromId]);
    await client.query('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [amount, toId]);
    await client.query('INSERT INTO transfers (from_id, to_id, amount) VALUES ($1, $2, $3)', [fromId, toId, amount]);
});
```

### 5. **Monitor Performance**
```javascript
// Set up alerts for slow queries
const SLOW_QUERY_THRESHOLD = 5000; // 5 seconds

// Use appropriate batch sizes for bulk operations
const BATCH_SIZE = process.env.NODE_ENV === 'production' ? 1000 : 100;
```

### 6. **Environment-Specific Configuration**
```javascript
const config = {
    development: {
        defaultTimeout: 60000,  // Longer for debugging
        logLevel: 'debug'
    },
    production: {
        defaultTimeout: 30000,  // Faster for production
        logLevel: 'info'
    },
    test: {
        defaultTimeout: 5000,   // Quick for tests
        logLevel: 'error'
    }
};
```

---

## Common Patterns

### 1. **Search with Filters and Sorting**
```javascript
const searchProducts = async (filters) => {
    const { search, category, priceRange, sortBy, page } = filters;
    
    let patterns = {};
    let where = {};
    let ranges = {};
    
    if (search) {
        patterns.name = { type: 'contains', value: search };
    }
    
    if (category) {
        where.category = category;
    }
    
    if (priceRange) {
        ranges.price = priceRange;
    }
    
    // Combine different query types
    let results = [];
    
    if (Object.keys(patterns).length > 0) {
        results = await patternMatch('products', patterns, {
            orderBy: sortBy || 'created_at DESC'
        });
    } else {
        results = await advancedQuery('products', {
            where,
            orderBy: sortBy || 'created_at DESC'
        });
    }
    
    if (Object.keys(ranges).length > 0) {
        const rangeResults = await rangeQuery('products', ranges);
        // Intersect results if needed
    }
    
    return paginatedResults(results, page);
};
```

### 2. **Analytics Dashboard Data**
```javascript
const getDashboardData = async (dateRange) => {
    const [
        totalRevenue,
        orderStats,
        topProducts,
        customerGrowth
    ] = await Promise.all([
        // Total revenue
        aggregateQuery('orders', {
            aggregates: { total: 'SUM(amount)' },
            where: { status: 'completed' }
        }),
        
        // Order statistics
        groupByQuery('orders', {
            groupBy: 'DATE(created_at)',
            aggregates: {
                daily_orders: 'COUNT(*)',
                daily_revenue: 'SUM(amount)'
            }
        }),
        
        // Top products
        getTopRecords('products', {
            limit: 10,
            orderBy: 'sales_count DESC'
        }),
        
        // Customer growth
        windowQuery('users', {
            select: 'DATE(created_at) as signup_date',
            windowFunctions: [{
                func: 'COUNT',
                alias: 'cumulative_users',
                orderBy: 'created_at',
                frameClause: 'ROWS UNBOUNDED PRECEDING'
            }]
        })
    ]);
    
    return {
        revenue: totalRevenue.total,
        orders: orderStats,
        products: topProducts,
        growth: customerGrowth
    };
};
```

---

## Migration and Maintenance

### Data Migration Example
```javascript
const migrateUserData = async () => {
    return await executeTransaction(async (client) => {
        // Backup existing data
        await client.query(`
            CREATE TABLE users_backup AS 
            SELECT * FROM users WHERE created_at < NOW() - INTERVAL '1 year'
        `);
        
        // Migrate data
        const oldUsers = await client.query(`
            SELECT * FROM users_backup
        `);
        
        // Process in batches
        await bulkInsert('users_archive', oldUsers.rows, {
            batchSize: 1000,
            onConflict: 'ignore'
        });
        
        // Clean up old data
        await client.query(`
            DELETE FROM users WHERE created_at < NOW() - INTERVAL '1 year'
        `);
        
        return { migrated: oldUsers.rowCount };
    }, {
        timeout: 600000, // 10 minutes
        isolationLevel: 'SERIALIZABLE'
    });
};
```

This documentation covers all major features and provides practical examples for using the enhanced database utilities in production applications. 