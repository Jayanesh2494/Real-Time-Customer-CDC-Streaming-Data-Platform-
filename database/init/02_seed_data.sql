-- =============================================================================
-- Initial Seed Data for E-Commerce Database
-- Provides realistic operational records for immediate testing & CDC baseline
-- =============================================================================

-- 1. Seed Customers
INSERT INTO customers (customer_id, first_name, last_name, email, city, state, created_at, updated_at) VALUES
(1, 'Aarav', 'Sharma', 'aarav.sharma@example.com', 'Bengaluru', 'Karnataka', CURRENT_TIMESTAMP - INTERVAL '30 days', CURRENT_TIMESTAMP - INTERVAL '30 days'),
(2, 'Diya', 'Patel', 'diya.patel@example.com', 'Mumbai', 'Maharashtra', CURRENT_TIMESTAMP - INTERVAL '25 days', CURRENT_TIMESTAMP - INTERVAL '25 days'),
(3, 'Rohan', 'Verma', 'rohan.verma@example.com', 'Delhi', 'Delhi', CURRENT_TIMESTAMP - INTERVAL '20 days', CURRENT_TIMESTAMP - INTERVAL '20 days'),
(4, 'Ananya', 'Iyer', 'ananya.iyer@example.com', 'Chennai', 'Tamil Nadu', CURRENT_TIMESTAMP - INTERVAL '15 days', CURRENT_TIMESTAMP - INTERVAL '15 days'),
(5, 'Vikram', 'Reddy', 'vikram.reddy@example.com', 'Hyderabad', 'Telangana', CURRENT_TIMESTAMP - INTERVAL '10 days', CURRENT_TIMESTAMP - INTERVAL '10 days'),
(6, 'Pooja', 'Nair', 'pooja.nair@example.com', 'Kochi', 'Kerala', CURRENT_TIMESTAMP - INTERVAL '8 days', CURRENT_TIMESTAMP - INTERVAL '8 days'),
(7, 'Aditya', 'Mehta', 'aditya.mehta@example.com', 'Ahmedabad', 'Gujarat', CURRENT_TIMESTAMP - INTERVAL '5 days', CURRENT_TIMESTAMP - INTERVAL '5 days'),
(8, 'Sneha', 'Mukherjee', 'sneha.mukherjee@example.com', 'Kolkata', 'West Bengal', CURRENT_TIMESTAMP - INTERVAL '3 days', CURRENT_TIMESTAMP - INTERVAL '3 days'),
(9, 'Kabir', 'Singh', 'kabir.singh@example.com', 'Chandigarh', 'Punjab', CURRENT_TIMESTAMP - INTERVAL '2 days', CURRENT_TIMESTAMP - INTERVAL '2 days'),
(10, 'Neha', 'Gupta', 'neha.gupta@example.com', 'Pune', 'Maharashtra', CURRENT_TIMESTAMP - INTERVAL '1 days', CURRENT_TIMESTAMP - INTERVAL '1 days')
ON CONFLICT (customer_id) DO NOTHING;

-- Reset sequence for customers
SELECT setval('customers_customer_id_seq', (SELECT MAX(customer_id) FROM customers));

-- 2. Seed Products
INSERT INTO products (product_id, product_name, category, price, stock_quantity, created_at, updated_at) VALUES
(1, 'Ultra HD 4K Smart Monitor 27"', 'Electronics', 24999.00, 150, CURRENT_TIMESTAMP - INTERVAL '40 days', CURRENT_TIMESTAMP - INTERVAL '40 days'),
(2, 'Wireless Noise Cancelling Headphones', 'Electronics', 12499.00, 300, CURRENT_TIMESTAMP - INTERVAL '40 days', CURRENT_TIMESTAMP - INTERVAL '40 days'),
(3, 'Ergonomic Mesh Office Chair', 'Furniture', 8999.00, 80, CURRENT_TIMESTAMP - INTERVAL '35 days', CURRENT_TIMESTAMP - INTERVAL '35 days'),
(4, 'Mechanical Gaming Keyboard RGB', 'Accessories', 4599.00, 220, CURRENT_TIMESTAMP - INTERVAL '35 days', CURRENT_TIMESTAMP - INTERVAL '35 days'),
(5, 'Precision Wireless Mouse', 'Accessories', 1999.00, 400, CURRENT_TIMESTAMP - INTERVAL '30 days', CURRENT_TIMESTAMP - INTERVAL '30 days'),
(6, 'Stainless Steel Thermal Water Bottle 1L', 'Home & Living', 799.00, 600, CURRENT_TIMESTAMP - INTERVAL '30 days', CURRENT_TIMESTAMP - INTERVAL '30 days'),
(7, 'Smart Fitness Tracker Band', 'Wearables', 2999.00, 250, CURRENT_TIMESTAMP - INTERVAL '25 days', CURRENT_TIMESTAMP - INTERVAL '25 days'),
(8, 'USB-C Multiport Docking Hub 7-in-1', 'Accessories', 3499.00, 180, CURRENT_TIMESTAMP - INTERVAL '20 days', CURRENT_TIMESTAMP - INTERVAL '20 days'),
(9, 'Organic Cotton Crewneck T-Shirt', 'Apparel', 999.00, 500, CURRENT_TIMESTAMP - INTERVAL '15 days', CURRENT_TIMESTAMP - INTERVAL '15 days'),
(10, 'Compact Air Purifier with HEPA Filter', 'Home & Living', 6499.00, 95, CURRENT_TIMESTAMP - INTERVAL '10 days', CURRENT_TIMESTAMP - INTERVAL '10 days')
ON CONFLICT (product_id) DO NOTHING;

-- Reset sequence for products
SELECT setval('products_product_id_seq', (SELECT MAX(product_id) FROM products));

-- 3. Seed Orders
INSERT INTO orders (order_id, customer_id, order_status, total_amount, order_date, updated_at) VALUES
(1001, 1, 'DELIVERED', 26998.00, CURRENT_TIMESTAMP - INTERVAL '28 days', CURRENT_TIMESTAMP - INTERVAL '24 days'),
(1002, 2, 'DELIVERED', 12499.00, CURRENT_TIMESTAMP - INTERVAL '22 days', CURRENT_TIMESTAMP - INTERVAL '18 days'),
(1003, 3, 'DELIVERED', 10998.00, CURRENT_TIMESTAMP - INTERVAL '18 days', CURRENT_TIMESTAMP - INTERVAL '14 days'),
(1004, 4, 'SHIPPED', 6598.00, CURRENT_TIMESTAMP - INTERVAL '10 days', CURRENT_TIMESTAMP - INTERVAL '7 days'),
(1005, 5, 'CONFIRMED', 24999.00, CURRENT_TIMESTAMP - INTERVAL '6 days', CURRENT_TIMESTAMP - INTERVAL '5 days'),
(1006, 6, 'CONFIRMED', 3798.00, CURRENT_TIMESTAMP - INTERVAL '4 days', CURRENT_TIMESTAMP - INTERVAL '3 days'),
(1007, 7, 'PLACED', 8999.00, CURRENT_TIMESTAMP - INTERVAL '2 days', CURRENT_TIMESTAMP - INTERVAL '2 days'),
(1008, 8, 'PLACED', 3499.00, CURRENT_TIMESTAMP - INTERVAL '1 days', CURRENT_TIMESTAMP - INTERVAL '1 days'),
(1009, 9, 'CANCELLED', 1999.00, CURRENT_TIMESTAMP - INTERVAL '1 days', CURRENT_TIMESTAMP - INTERVAL '1 days'),
(1010, 10, 'DELIVERED', 7498.00, CURRENT_TIMESTAMP - INTERVAL '12 hours', CURRENT_TIMESTAMP - INTERVAL '6 hours')
ON CONFLICT (order_id) DO NOTHING;

-- Reset sequence for orders
SELECT setval('orders_order_id_seq', (SELECT MAX(order_id) FROM orders));

-- 4. Seed Order Items
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price) VALUES
(1, 1001, 1, 1, 24999.00),
(2, 1001, 5, 1, 1999.00),
(3, 1002, 2, 1, 12499.00),
(4, 1003, 3, 1, 8999.00),
(5, 1003, 5, 1, 1999.00),
(6, 1004, 4, 1, 4599.00),
(7, 1004, 5, 1, 1999.00),
(8, 1005, 1, 1, 24999.00),
(9, 1006, 7, 1, 2999.00),
(10, 1006, 6, 1, 799.00),
(11, 1007, 3, 1, 8999.00),
(12, 1008, 8, 1, 3499.00),
(13, 1009, 5, 1, 1999.00),
(14, 1010, 10, 1, 6499.00),
(15, 1010, 9, 1, 999.00)
ON CONFLICT (order_item_id) DO NOTHING;

-- Reset sequence for order_items
SELECT setval('order_items_order_item_id_seq', (SELECT MAX(order_item_id) FROM order_items));

-- 5. Seed Payments
INSERT INTO payments (payment_id, order_id, payment_method, payment_status, payment_amount, payment_date) VALUES
(501, 1001, 'CREDIT_CARD', 'SUCCESS', 26998.00, CURRENT_TIMESTAMP - INTERVAL '28 days'),
(502, 1002, 'UPI', 'SUCCESS', 12499.00, CURRENT_TIMESTAMP - INTERVAL '22 days'),
(503, 1003, 'NET_BANKING', 'SUCCESS', 10998.00, CURRENT_TIMESTAMP - INTERVAL '18 days'),
(504, 1004, 'UPI', 'SUCCESS', 6598.00, CURRENT_TIMESTAMP - INTERVAL '10 days'),
(505, 1005, 'DEBIT_CARD', 'SUCCESS', 24999.00, CURRENT_TIMESTAMP - INTERVAL '6 days'),
(506, 1006, 'UPI', 'SUCCESS', 3798.00, CURRENT_TIMESTAMP - INTERVAL '4 days'),
(507, 1007, 'CREDIT_CARD', 'PENDING', 8999.00, CURRENT_TIMESTAMP - INTERVAL '2 days'),
(508, 1008, 'WALLET', 'SUCCESS', 3499.00, CURRENT_TIMESTAMP - INTERVAL '1 days'),
(509, 1009, 'UPI', 'FAILED', 1999.00, CURRENT_TIMESTAMP - INTERVAL '1 days'),
(510, 1010, 'CREDIT_CARD', 'SUCCESS', 7498.00, CURRENT_TIMESTAMP - INTERVAL '12 hours')
ON CONFLICT (payment_id) DO NOTHING;

-- Reset sequence for payments
SELECT setval('payments_payment_id_seq', (SELECT MAX(payment_id) FROM payments));
