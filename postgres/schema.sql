CREATE TABLE IF NOT EXISTS locations (
    zip_code VARCHAR(10) PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    state_code CHAR(2) NOT NULL,
    state_name VARCHAR(100) NOT NULL,
    country VARCHAR(50) DEFAULT 'USA'
);

-- 2. Merchants
CREATE TABLE IF NOT EXISTS merchants (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    category VARCHAR(100) NOT NULL
);

-- 3. Customers (Added birth_date)
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    birth_date DATE NOT NULL,
    address VARCHAR(255),
    zip_code VARCHAR(10) REFERENCES locations(zip_code),
    marital_status VARCHAR(50),
    education_level VARCHAR(100),
    income_category VARCHAR(100),
    estimated_net_worth NUMERIC(18,2),
    employment_status VARCHAR(100),
    created_at TIMESTAMP DEFAULT (now() AT TIME ZONE 'UTC')
);

-- 4. Accounts
CREATE TABLE IF NOT EXISTS accounts (
    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    account_type VARCHAR(50) NOT NULL,
    account_status VARCHAR(20) DEFAULT 'ACTIVE',
    balance NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    created_at TIMESTAMP DEFAULT (now() AT TIME ZONE 'UTC')
);

-- 5. Transactions
CREATE TABLE IF NOT EXISTS transactions (
    id BIGSERIAL PRIMARY KEY,
    account_id INT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    merchant_id INT REFERENCES merchants(id),
    transaction_type VARCHAR(50) NOT NULL,
    amount NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    related_account_id INT NULL REFERENCES accounts(id),
    status VARCHAR(20) NOT NULL DEFAULT 'COMPLETED',
    is_high_value BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT (now() AT TIME ZONE 'UTC')
);