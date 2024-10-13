SELECT  
    DISTINCT county
FROM 
    uk_price_paid;

-- Lab Question
SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM uk_price_paid
WHERE county = 'GREATER LONDON' AND date < toDate('2024-01-01');

ALTER TABLE uk_price_paid
    ADD INDEX county_index county
    TYPE set(10)
    GRANULARITY 5;

ALTER TABLE uk_price_paid
    MATERIALIZE INDEX county_index;

SELECT 
    *
FROM 
    system.mutations;

SELECT
    table,
    formatReadableSize(data_compressed_bytes) as data_compressed,
    formatReadableSize(secondary_indices_compressed_bytes) as index_compressed,
    formatReadableSize(primary_key_size) as primary_key
FROM
    system.parts
ORDER BY secondary_indices_uncompressed_bytes DESC
LIMIT 5;

ALTER TABLE uk_price_paid
    DROP INDEX county_index;

ALTER TABLE uk_price_paid
    ADD INDEX county_index county
    TYPE set(10)
    GRANULARITY 1;

ALTER TABLE uk_price_paid
    MATERIALIZE INDEX county_index;

SELECT 
    *
FROM
    system.mutations;

SELECT
    table,
    formatReadableSize(data_compressed_bytes) as data_compressed,
    formatReadableSize(secondary_indices_compressed_bytes) as index_compressed,
    formatReadableSize(primary_key_size) as primary_key
FROM
    system.parts
ORDER BY secondary_indices_uncompressed_bytes DESC
LIMIT 5;

EXPLAIN indexes = 1 
SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM 
    uk_price_paid
WHERE 
    county = 'GREATER LONDON';


-- Lab 12.2
-- Lab Question
SELECT 
    toYear(date) AS year,
    count(),
    avg(price),
    max(price),
    formatReadableQuantity(sum(price))
FROM uk_price_paid
WHERE town = 'LIVERPOOL'
GROUP BY year
ORDER BY year DESC;

SELECT
    formatReadableSize(sum(bytes_on_disk)),
    count() AS num_of_parts
FROM system.parts
WHERE table = 'uk_price_paid' AND active = 1;

ALTER TABLE uk_price_paid
    ADD PROJECTION town_date_projection (
        SELECT
            town, date, price
        ORDER BY town,date
    );

ALTER TABLE uk_price_paid
    MATERIALIZE PROJECTION town_date_projection;

SELECT
    *
FROM
    system.mutations;

ALTER TABLE uk_price_paid
    ADD PROJECTION handy_aggs_projection (
        SELECT
            avg(price),
            max(price),
            sum(price)
        GROUP BY town
    );

ALTER TABLE uk_price_paid
    MATERIALIZE PROJECTION handy_aggs_projection;

SELECT
    *
FROM
    system.mutations;

-- Lab Question
SELECT 
    avg(price),
    max(price),
    formatReadableQuantity(sum(price))
FROM uk_price_paid
WHERE town = 'LIVERPOOL';

EXPLAIN 
SELECT
    avg(price),
    max(price),
    formatReadableQuantity(sum(price))
FROM 
    uk_price_paid
WHERE 
    town = 'LIVERPOOL';