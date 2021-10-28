USE test_db;

-- CREATE Sales TABLE
CREATE TABLE Sales
(
	ticket_id INT,
    trans_date DATETIME,
    event_id INT,
    event_name VARCHAR(50),
    event_date DATETIME,
	event_type VARCHAR(10),
    event_city VARCHAR(20),
    customer_id INT,
    price DECIMAL,
    num_tickets INT,
	PRIMARY KEY (ticket_id)
);

-- SELECT event_name
-- FROM Sales
-- GROUP BY event_name
-- ORDER BY COUNT(*) DESC, event_id
-- LIMIT 3;

-- select * from Sales;
