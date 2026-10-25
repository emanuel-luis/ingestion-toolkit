-- Source changes between runs: 10 new orders, 5 customers updated, 3 customers inserted,
-- 4 products updated and 2 inserted (at the end of the file).
-- Every touched customer gets an updated_at past the previous maximum, so the incremental
-- watermark picks them up.
INSERT INTO sales.orders
SELECT i,
       (i - 1) % 200 + 1,
       (i - 1) % 100 + 1,
       i % 7 + 1,
       round(((i % 7 + 1) * (5 + (i % 13) * 2.5))::numeric, 2),
       date '2025-06-01' + i % 180
FROM generate_series(1001, 1010) AS i;

UPDATE sales.customers
SET city = 'Newtown',
    updated_at = (SELECT max(updated_at) FROM sales.customers) + interval '1 hour'
WHERE id IN (1, 2, 3, 4, 5);

INSERT INTO sales.customers
SELECT i,
       'Customer ' || lpad(i::text, 4, '0'),
       'customer' || lpad(i::text, 4, '0') || '@example.com',
       'Newtown',
       timestamp '2025-01-01 09:00' + i * interval '1 day',
       (SELECT max(updated_at) FROM sales.customers) + interval '2 hours'
FROM generate_series(201, 203) AS i;

-- Products: 4 updated, 2 inserted. The trigger gives each touched row a new version_stamp,
-- so the CDC extract picks up exactly these 6 rows.
UPDATE sales.products SET category = 'Clearance' WHERE id IN (1, 2, 3, 4);

INSERT INTO sales.products (id, name, category, price, version_stamp)
SELECT i, 'Product ' || lpad(i::text, 3, '0'), 'Tools', 9.99, 0
FROM generate_series(101, 102) AS i;
