CREATE DATABASE IF NOT EXISTS cdc_db;
USE cdc_db;

CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    stock INT NOT NULL DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO products (name, description, price, stock) VALUES
('Laptop', 'High-performance laptop', 1200.00, 50),
('Mouse', 'Wireless ergonomic mouse', 25.00, 200),
('Keyboard', 'Mechanical gaming keyboard', 75.00, 100);
