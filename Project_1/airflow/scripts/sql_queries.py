create_table_query = """
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    order_status VARCHAR(50),
    order_purchase_timestamp DATETIME
);
"""

insert_data_query = """
INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp) 
VALUES (%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
    customer_id = VALUES(customer_id),
    order_status = VALUES(order_status),
    order_purchase_timestamp = VALUES(order_purchase_timestamp);
"""