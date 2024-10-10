SELECT 
    *
FROM 
    s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv');

CREATE DICTIONARY uk_mortgage_rates (
    date DateTime64,
    variable Decimal32(2),
    fixed Decimal32(2),
    bank Decimal32(2)
)
PRIMARY KEY date
SOURCE(
  HTTP(
   url 'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv'
   format 'CSVWithNames'
  )
)
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(2628000000);

SELECT 
    count()
FROM 
    uk_mortgage_rates;

SELECT 
    *
FROM 
    uk_mortgage_rates;

WITH
    toStartOfMonth(uk_price_paid.date) AS month
SELECT
    month,
    count(),
    any(variable)
FROM 
    uk_price_paid
JOIN 
    uk_mortgage_rates
    ON month = toStartOfMonth(uk_mortgage_rates.date)
GROUP BY 
    month;

WITH
    toStartOfMonth(uk_price_paid.date) AS month
SELECT
    month,
    count() AS volume,
    any(variable)
FROM 
    uk_price_paid
JOIN 
    uk_mortgage_rates
    ON month = toStartOfMonth(uk_mortgage_rates.date)
GROUP BY 
    month
ORDER BY
    volume DESC;

SELECT
    corr(toFloat32(volume),toFloat32(variable))
FROM (
    WITH
        toStartOfMonth(uk_price_paid.date) AS month
    SELECT
        month,
        count() AS volume,
        any(variable) AS variable
    FROM 
        uk_price_paid
    JOIN 
        uk_mortgage_rates
        ON month = toStartOfMonth(uk_mortgage_rates.date)
    GROUP BY 
        month
);

SELECT
    corr(toFloat32(volume),toFloat32(fixed))
FROM (
    WITH
        toStartOfMonth(uk_price_paid.date) AS month
    SELECT
        month,
        count() AS volume,
        any(fixed) AS fixed
    FROM 
        uk_price_paid
    JOIN 
        uk_mortgage_rates
        ON month = toStartOfMonth(uk_mortgage_rates.date)
    GROUP BY 
        month
);