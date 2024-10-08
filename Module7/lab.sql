SELECT 
    town,
    sum(price) AS sum_price,
    formatReadableQuantity(sum_price)
FROM uk_price_paid
GROUP BY town
ORDER BY sum_price DESC;

CREATE TABLE prices_sum_dest (
    town LowCardinality(String),
    sum_price UInt64
)
ENGINE = SummingMergeTree
PRIMARY KEY town;

CREATE MATERIALIZED VIEW prices_sum_view
TO prices_sum_dest
AS
    SELECT
        town,
        sum(price) AS sum_price
    FROM 
        uk_price_paid
    GROUP BY 
        town;

INSERT INTO prices_sum_dest
    SELECT
        town,
        sum(price) AS sum_price
    FROM uk_price_paid
    GROUP BY town;

SELECT 
    count()
FROM
    prices_sum_dest;

SELECT 
    town,
    sum(price) AS sum_price,
    formatReadableQuantity(sum_price)
FROM uk_price_paid
WHERE town = 'LONDON'
GROUP BY town;

SELECT
    town,
    sum_price AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
WHERE town = 'LONDON';

INSERT INTO uk_price_paid (price, date, town, street)
VALUES
    (4294967295, toDate('2024-01-01'), 'LONDON', 'My Street1');

SELECT
    town,
    sum(sum_price) AS sum,
    formatReadableQuantity(sum)
FROM 
    prices_sum_dest
WHERE 
    town = 'LONDON'
GROUP BY 
    town;

SELECT
    town,
    sum(sum_price) AS sum,
    formatReadableQuantity(sum)
FROM 
    prices_sum_dest
GROUP BY 
    town
ORDER BY 
    sum DESC
LIMIT 
    10;


-- Lab 7.2
WITH
    toStartOfMonth(date) AS month
SELECT 
    month,
    min(price) AS min_price,
    max(price) AS max_price
FROM uk_price_paid
GROUP BY month 
ORDER BY month DESC;

WITH
    toStartOfMonth(date) AS month
SELECT 
    month,
    avg(price)
FROM uk_price_paid
GROUP BY month 
ORDER BY month DESC;

WITH
    toStartOfMonth(date) AS month
SELECT 
    month,
    count()
FROM uk_price_paid
GROUP BY month 
ORDER BY month DESC;

CREATE TABLE uk_prices_aggs_dest (
    month Date,
    min_price SimpleAggregateFunction(min, UInt32),
    max_price SimpleAggregateFunction(max, UInt32),
    volume AggregateFunction(count, UInt32),
    avg_price AggregateFunction(avg, UInt32)
)
ENGINE = AggregatingMergeTree
PRIMARY KEY month;

CREATE MATERIALIZED VIEW uk_prices_aggs_view
TO uk_prices_aggs_dest
AS
    WITH
        toStartOfMonth(date) AS month
    SELECT
        month,
        minSimpleState(price) AS min_price,
        maxSimpleState(price) AS max_price,
        countState(price) AS volume,
        avgState(price) AS avg_price
    FROM 
        uk_price_paid
    GROUP BY 
        month;

INSERT INTO uk_prices_aggs_dest
    WITH
        toStartOfMonth(date) AS month
    SELECT
        month,
        minSimpleState(price) AS min_price,
        maxSimpleState(price) AS max_price,
        countState(price) AS volume,
        avgState(price) AS avg_price
    FROM 
        uk_price_paid
    WHERE 
        date < toDate('2024-01-01')
    GROUP BY 
        month;

SELECT * FROM uk_prices_aggs_dest;

SELECT
    month,
    min(min_price),
    max(max_price)
FROM 
    uk_prices_aggs_dest
WHERE
    month >= (toStartOfMonth(now()) - (INTERVAL 12 MONTH))
    AND month < toStartOfMonth(now())
GROUP BY 
    month
ORDER BY 
    month DESC;

SELECT
    month,
    avgMerge(avg_price)
FROM 
    uk_prices_aggs_dest
WHERE
    month >= (toStartOfMonth(now()) - (INTERVAL 2 YEAR))
    AND month < toStartOfMonth(now())
GROUP BY 
    month
ORDER BY 
    month DESC;

SELECT
    countMerge(volume)
FROM 
    uk_prices_aggs_dest
WHERE 
    toYear(month) = '2020';

INSERT INTO uk_price_paid (date, price, town) VALUES
    ('2024-08-01', 10000, 'Little Whinging'),
    ('2024-08-01', 1, 'Little Whinging');

SELECT 
    month,
    countMerge(volume),
    min(min_price),
    max(max_price),
    avgMerge(avg_price)
FROM 
    uk_prices_aggs_dest
WHERE 
    toYYYYMM(month) = '202408'
GROUP BY 
    month;