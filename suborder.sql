-- Portal suborder table schema
-- Data is populated via Linear webhook (main.py /webhooks/linear endpoint)

CREATE SCHEMA IF NOT EXISTS portal;

CREATE TABLE IF NOT EXISTS portal.suborder (
    linear_id UUID PRIMARY KEY,
    order_linear_id UUID NOT NULL,
    utilities TEXT,
    provider TEXT,
    scheduled_for DATE,
    status TEXT DEFAULT 'TODO'
);

CREATE INDEX IF NOT EXISTS idx_suborder_order_linear_id ON portal.suborder(order_linear_id);
