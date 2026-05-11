-- Test database schema for e-commerce example

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_total DECIMAL(10, 2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending'
);

CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL
);

-- Insert sample data
INSERT INTO customers (email, first_name, last_name) VALUES
    ('john@example.com', 'John', 'Doe'),
    ('jane@example.com', 'Jane', 'Smith'),
    ('bob@example.com', 'Bob', 'Johnson');

INSERT INTO products (product_name, price, category) VALUES
    ('Widget A', 29.99, 'Electronics'),
    ('Widget B', 49.99, 'Electronics'),
    ('Gadget X', 19.99, 'Accessories');

INSERT INTO orders (customer_id, order_total, order_date) VALUES
    (1, 79.98, '2024-01-15'),
    (2, 29.99, '2024-01-20'),
    (1, 49.99, '2024-02-01');

INSERT INTO order_items (order_id, product_id, quantity, price) VALUES
    (1, 1, 1, 29.99),
    (1, 2, 1, 49.99),
    (2, 1, 1, 29.99),
    (3, 2, 1, 49.99);

-- Add some nulls for quality testing
INSERT INTO customers (email, first_name, last_name) VALUES
    ('incomplete@example.com', NULL, NULL);

-- Add duplicate for testing
INSERT INTO orders (customer_id, order_total, order_date) VALUES
    (1, 79.98, '2024-01-15');
