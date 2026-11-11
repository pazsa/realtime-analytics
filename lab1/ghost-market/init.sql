CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'ghost-market';
CREATE TABLE orders (
	id TEXT PRIMARY KEY,
	campaign_id TEXT NOT NULL,
	status TEXT NOT NULL,
	amount NUMERIC(12,2) NOT NULL,
	created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE order_items (
	id TEXT PRIMARY KEY,
	order_id TEXT NOT NULL REFERENCES orders(id),
	sku TEXT NOT NULL,
	quantity INTEGER NOT NULL,
	unit_price NUMERIC(12,2) NOT NULL
);
CREATE TABLE payments (
	id TEXT PRIMARY KEY,
	order_id TEXT NOT NULL REFERENCES orders(id),
	status TEXT NOT NULL,
	amount NUMERIC(12,2) NOT NULL,
	paid_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE internal_notes(
	id TEXT PRIMARY KEY,
	note TEXT NOT NULL
);

ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
ALTER TABLE payments REPLICA IDENTITY FULL;
 
CREATE PUBLICATION ghost_market_pub FOR TABLE orders, order_items, payments;

GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON orders, order_items, payments, internal_notes TO debezium;

INSERT INTO orders
VALUES('seed_order_01', 'chiba_retina', 'paid', 149.90, '2026-01-01T03:17:00Z');

INSERT INTO order_items
VALUES('seed_item_01', 'seed_order_01','maternal_pride_subscription', 1, 149.90);

INSERT INTO payments
VALUES('seed_payment_01', 'seed_order_01', 'captured', 149.90, '2026-01-01T03:18:00Z');

INSERT INTO internal_notes
VALUES('seed_note_01', 'do-not-export');
