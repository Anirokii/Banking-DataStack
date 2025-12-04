-- =============================================
-- Banking Database Schema
-- =============================================

-- Drop tables if they exist (for clean restart)
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- =============================================
-- Table: customers
-- =============================================
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index on email for faster lookups
CREATE INDEX idx_customers_email ON customers(email);

-- =============================================
-- Table: accounts
-- =============================================
CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    account_type VARCHAR(20) NOT NULL CHECK (account_type IN ('SAVINGS', 'CHECKING')),
    balance NUMERIC(15, 2) DEFAULT 0.00 NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD' NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT positive_balance CHECK (balance >= 0)
);

-- Indexes for faster queries
CREATE INDEX idx_accounts_customer_id ON accounts(customer_id);
CREATE INDEX idx_accounts_type ON accounts(account_type);

-- =============================================
-- Table: transactions
-- =============================================
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    txn_type VARCHAR(20) NOT NULL CHECK (txn_type IN ('DEPOSIT', 'WITHDRAWAL', 'TRANSFER')),
    amount NUMERIC(15, 2) NOT NULL,
    related_account_id INTEGER REFERENCES accounts(id) ON DELETE SET NULL,
    status VARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT positive_amount CHECK (amount > 0)
);

-- Indexes for faster queries
CREATE INDEX idx_transactions_account_id ON transactions(account_id);
CREATE INDEX idx_transactions_type ON transactions(txn_type);
CREATE INDEX idx_transactions_status ON transactions(status);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);

-- =============================================
-- Sample verification queries
-- =============================================
-- You can uncomment and run these after data generation:

-- SELECT COUNT(*) FROM customers;
-- SELECT COUNT(*) FROM accounts;
-- SELECT COUNT(*) FROM transactions;

-- SELECT c.first_name, c.last_name, COUNT(a.id) as num_accounts, SUM(a.balance) as total_balance
-- FROM customers c
-- LEFT JOIN accounts a ON c.id = a.customer_id
-- GROUP BY c.id, c.first_name, c.last_name
-- ORDER BY total_balance DESC
-- LIMIT 10;