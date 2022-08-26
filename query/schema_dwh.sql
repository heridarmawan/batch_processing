DROP TABLE IF EXISTS dim_users;
CREATE TABLE IF NOT EXISTS dim_users (
	user_id INT NOT NULL,
	first_name VARCHAR(255) NOT NULL,
	last_name VARCHAR(255) NOT NULL,
	gender VARCHAR(50) NOT NULL,
	address VARCHAR(255),
	tgl_lahir DATE NOT NULL,
	tgl_join DATE NOT NULL,
	PRIMARY KEY (user_id)
);
    
DROP TABLE IF EXISTS fact_orders;
CREATE TABLE IF NOT EXISTS fact_orders (
	order_id INT NOT NULL,
	order_date DATE NOT NULL,
    user_id INT NOT NULL,
    order_price INT NOT NULL,
	order_discount INT,
	voucher_id INT,
	order_total INT NOT NULL
);