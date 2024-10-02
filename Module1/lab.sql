SELECT
    *
FROM
    s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
LIMIT
    100;

SELECT
    count()
FROM
    s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet');

SELECT
    formatReadableQuantity(count())
FROM
    s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet');

SELECT
    avg(volume)
FROM
    s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
WHERE
    crypto_name = 'Bitcoin';

SELECT
    crypto_name,
    count(trade_date)
FROM
    s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
GROUP BY
    crypto_name
ORDER BY
    crypto_name;

SELECT
    trim(crypto_name),
    count(trade_date)
FROM
    s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
GROUP BY
    crypto_name
ORDER BY
    crypto_name DESC;