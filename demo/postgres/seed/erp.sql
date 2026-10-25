-- Deterministic synthetic source: no random(), no now(). Re-running it on an empty database
-- always produces the same rows.
CREATE SCHEMA sales;

CREATE TABLE sales.customers (
    id          integer PRIMARY KEY,
    name        text,
    email       text,
    city        text,
    created_at  timestamp,
    updated_at  timestamp
);

CREATE TABLE sales.orders (
    id           integer PRIMARY KEY,
    customer_id  integer,
    product_id   integer,
    quantity     integer,      -- nullable here on purpose: one row shows the contract reject
    total        numeric(12, 2),
    order_date   date
);

INSERT INTO sales.customers
SELECT i,
       'Customer ' || lpad(i::text, 4, '0'),
       'customer' || lpad(i::text, 4, '0') || '@example.com',
       (ARRAY['Springfield', 'Riverton', 'Lakeside', 'Hillcrest', 'Fairview'])[i % 5 + 1],
       timestamp '2025-01-01 09:00' + i * interval '1 day',
       timestamp '2025-12-01 08:00' + i * interval '1 hour'
FROM generate_series(1, 200) AS i;

INSERT INTO sales.orders
SELECT i,
       (i - 1) % 200 + 1,
       (i - 1) % 100 + 1,
       CASE WHEN i = 500 THEN NULL ELSE i % 7 + 1 END,
       round(((i % 7 + 1) * (5 + (i % 13) * 2.5))::numeric, 2),
       date '2025-06-01' + i % 180
FROM generate_series(1, 1000) AS i;

-- Products carry a change sequence: a trigger stamps every insert and update from one sequence,
-- which is what the CDC DAG reads. (A real source could use a log position or a rowversion.)
CREATE SEQUENCE sales.products_version_seq;

CREATE TABLE sales.products (
    id             integer PRIMARY KEY,
    name           text,
    category       text,
    price          numeric(10, 2),
    version_stamp  bigint NOT NULL
);

CREATE FUNCTION sales.stamp_product_version() RETURNS trigger AS $$
BEGIN
    NEW.version_stamp := nextval('sales.products_version_seq');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER products_stamp_version
BEFORE INSERT OR UPDATE ON sales.products
FOR EACH ROW EXECUTE FUNCTION sales.stamp_product_version();

INSERT INTO sales.products (id, name, category, price, version_stamp)
SELECT i,
       'Product ' || lpad(i::text, 3, '0'),
       (ARRAY['Tools', 'Garden', 'Kitchen', 'Toys'])[i % 4 + 1],
       round((5 + (i % 17) * 3.25)::numeric, 2),
       0  -- replaced by the trigger
FROM generate_series(1, 100) AS i;
