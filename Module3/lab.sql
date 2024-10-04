DESCRIBE pypi;

SELECT
    uniqExact(COUNTRY_CODE) AS uniq_country_code
FROM pypi;

SELECT
    uniqExact(PROJECT) AS uniq_project
FROM pypi;

SELECT
    uniqExact(URL) AS uniq_url
FROM pypi;

CREATE TABLE pypi3 (
    TIMESTAMP DateTime64,
    COUNTRY_CODE LowCardinality(String),
    PROJECT LowCardinality(String),
    URL String
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, TIMESTAMP);

INSERT INTO pypi3
    SELECT * FROM pypi2;

SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table LIKE 'pypi%')
GROUP BY table;

SELECT
    toStartOfMonth(TIMESTAMP) AS month,
    count() AS count
FROM pypi2
WHERE COUNTRY_CODE = 'US'
GROUP BY
    month
ORDER BY
    month ASC,
    count DESC;

SELECT
    toStartOfMonth(TIMESTAMP) AS month,
    count() AS count
FROM pypi3
WHERE COUNTRY_CODE = 'US'
GROUP BY
    month
ORDER BY
    month ASC,
    count DESC;


-- Lab 3.2
DESCRIBE s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet');

CREATE TABLE crypto_prices (
    trade_date Date,
    crypto_name LowCardinality(String),
    volume Float32,
    price Float32,
    market_cap Float32,
    change_1_day Float32
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name, trade_date);

INSERT INTO crypto_prices
    SELECT * 
    FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet');

SELECT
    count(*)
FROM
    crypto_prices;

SELECT 
    count(*)
FROM
    crypto_prices
WHERE 
    volume > 1000000;

SELECT 
    *
FROM
    crypto_prices
LIMIT
    1;

SELECT
    avg(price)
FROM
    crypto_prices
WHERE 
    crypto_name = 'Bitcoin';

SELECT
    avg(price)
FROM
    crypto_prices
WHERE 
    crypto_name LIKE 'B%';