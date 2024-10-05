SELECT 
    *
FROM 
    uk_price_paid
WHERE 
    price >= 100000000
ORDER BY 
    price DESC;

SELECT 
    count()
FROM 
    uk_price_paid
WHERE
    price > 1000000
    AND toYear(date) = '2022';

SELECT 
    uniqExact(town)
FROM 
    uk_price_paid;

SELECT
    town,
    count() AS c
FROM
    uk_price_paid
GROUP BY
    town
ORDER BY
    c DESC;

SELECT 
    topKIf(10)(town, town != 'LONDON')
FROM 
    uk_price_paid;

SELECT
    town,
    avg(price) AS avg_price
FROM 
    uk_price_paid
GROUP BY 
    town
ORDER BY 
    avg_price DESC
LIMIT 
    10;

SELECT 
    addr1, 
    addr2, 
    street, 
    town
FROM
    uk_price_paid
ORDER BY
    price DESC
LIMIT
    1;

SELECT
    type,
    avg(price)
FROM
    uk_price_paid
GROUP BY
    type;

SELECT
    sum(price)
FROM 
    uk_price_paid
WHERE
    county IN ['AVON','ESSEX','DEVON','KENT','CORNWALL']
    AND toYear(date) = '2020';

SELECT
    toStartOfMonth(date) AS month,
    avg(price) AS avg_price
FROM 
    uk_price_paid
WHERE
    date >= toDate('2005-01-01') AND date <= toDate('2010-12-31')
GROUP BY 
    month
ORDER BY 
    month ASC;

SELECT
    toStartOfDay(date) AS day,
    count()
FROM 
    uk_price_paid
WHERE
    town = 'LIVERPOOL'
    AND toYear(date) = '2020'
GROUP BY 
    day
ORDER BY 
    day ASC;

WITH (
    SELECT max(price)
    FROM uk_price_paid
) AS overall_price
SELECT
    town,
    max(price) / overall_price
FROM 
    uk_price_paid
GROUP BY 
    town
ORDER BY 
    2 DESC;