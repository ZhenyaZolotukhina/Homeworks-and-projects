CREATE TABLE users (
  user_id BIGINT NOT NULL,
  user_phone VARCHAR(32),
  PRIMARY KEY (user_id)
);

CREATE TABLE drivers (
  driver_id BIGINT NOT NULL,
  driver_phone VARCHAR(32),
  PRIMARY KEY (driver_id)
);

CREATE TABLE stores (
  store_id BIGINT NOT NULL,
  store_address VARCHAR(500),
  PRIMARY KEY (store_id)
);

CREATE TABLE items (
  item_id BIGINT NOT NULL,
  item_title VARCHAR(255),
  item_category VARCHAR(255),
  PRIMARY KEY (item_id)
);

CREATE TABLE orders (
  order_id BIGINT NOT NULL,
  address_text VARCHAR(500),
  created_at TIMESTAMP,
  paid_at TIMESTAMP,
  delivery_started_at TIMESTAMP,
  delivered_at TIMESTAMP,
  canceled_at TIMESTAMP,
  payment_type VARCHAR(64),
  order_discount NUMERIC(7,4),
  order_cancellation_reason VARCHAR(255),
  delivery_cost NUMERIC(19,4),
  user_id BIGINT,
  store_id BIGINT,
  driver_id BIGINT,
  PRIMARY KEY (order_id),
  FOREIGN KEY (user_id) REFERENCES users(user_id),
  FOREIGN KEY (store_id) REFERENCES stores(store_id),
  FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
);

CREATE TABLE driver_assignment (
  assignment_id BIGSERIAL NOT NULL,
  order_id BIGINT,
  driver_id BIGINT,
  delivered_at TIMESTAMP,
  PRIMARY KEY (assignment_id),
  FOREIGN KEY (order_id) REFERENCES orders(order_id),
  FOREIGN KEY (driver_id) REFERENCES drivers(driver_id)
);

CREATE TABLE order_items (
  order_item_id VARCHAR(64) NOT NULL,
  order_id BIGINT,
  item_id BIGINT,
  item_quantity NUMERIC(19,4),
  item_price NUMERIC(19,4),
  item_discount NUMERIC(7,4),
  item_canceled_quantity NUMERIC(19,4),
  replaces_order_item_id VARCHAR(64),
  PRIMARY KEY (order_item_id),
  FOREIGN KEY (order_id) REFERENCES orders(order_id),
  FOREIGN KEY (item_id) REFERENCES items(item_id)
);