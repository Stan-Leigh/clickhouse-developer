SELECT 
   count() AS count,
   by
FROM hackernews
GROUP BY by
ORDER BY count DESC;

DESCRIBE s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet');

CREATE TABLE uk_price_paid (
    price	UInt32,
    date	Date,
    postcode1	LowCardinality(String),
    postcode2	LowCardinality(String),
    type	Enum('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
    is_new	UInt8,
    duration	Enum('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
    addr1	String,
    addr2	String,
    street	LowCardinality(String),
    locality	LowCardinality(String),
    town	LowCardinality(String),
    district	LowCardinality(String),
    county	LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY (postcode1, postcode2, date);

INSERT INTO uk_price_paid
    SELECT * FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet');

SELECT
    count()
FROM
    uk_price_paid;

SELECT 
    avg(price)
FROM 
    uk_price_paid
WHERE 
    postcode1 = 'LU1' AND postcode2 = '5FT';

SELECT 
    avg(price)
FROM 
    uk_price_paid
WHERE 
    postcode2 = '5FT';

SELECT 
    avg(price)
FROM 
    uk_price_paid
WHERE 
    town = 'YORK';


-- LAB 4.3
SELECT 
    count()
FROM 
    s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS 
    format_csv_delimiter = '~';

SELECT 
    sum(actual_amount)
FROM 
    s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS 
    format_csv_delimiter = '~';

SELECT 
    sum(approved_amount)
FROM 
    s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS 
    format_csv_delimiter = '~';

DESCRIBE 
    s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS 
    format_csv_delimiter = '~';

SELECT
    sum(toUInt32OrZero(approved_amount)),
    sum(toUInt32OrZero(recommended_amount))
FROM 
    s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS 
    format_csv_delimiter = '~';

SELECT 
    formatReadableQuantity(sum(approved_amount)),
    formatReadableQuantity(sum(recommended_amount))
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS 
format_csv_delimiter='~',
schema_inference_hints='approved_amount UInt32, recommended_amount UInt32';

CREATE TABLE operating_budget (
    fiscal_year LowCardinality(String),
    service LowCardinality(String),
    department LowCardinality(String),
    program LowCardinality(String),
    program_code LowCardinality(String),
    description String,
    item_category LowCardinality(String),
    approved_amount UInt32,
    recommended_amount UInt32,
    actual_amount Decimal(12,2),
    fund LowCardinality(String),
    fund_type Enum('GENERAL FUNDS' = 1, 'FEDERAL FUNDS' = 2, 'OTHER FUNDS' = 3)
)
ENGINE = MergeTree
PRIMARY KEY (fiscal_year, program);

INSERT INTO operating_budget
    WITH
        splitByChar('(', c4) AS result
    SELECT
        c1 AS fiscal_year,
        c2 AS service,
        c3 AS department,
        result[1] AS program,
        splitByChar(')',result[2])[1] AS program_code,
        c5 AS description,
        c6 AS item_category,
        toUInt32OrZero(c7) AS approved_amount,
        toUInt32OrZero(c8) AS recommended_amount,
        toDecimal64(c9, 2) AS actual_amount,
        c10 AS fund,
        c11 AS fund_type
    FROM s3(
        'https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv',
        'CSV',
        'c1 String,
        c2 String,
        c3 String,
        c4 String,
        c5 String,
        c6 String,
        c7 String,
        c8 String,
        c9 String,
        c10 String,
        c11 String'
        )
    SETTINGS
        format_csv_delimiter = '~',
        input_format_csv_skip_first_lines=1;

SELECT
    *
FROM
    operating_budget;

SELECT 
    sum(approved_amount)
FROM 
    operating_budget
WHERE 
    fiscal_year = '2022';

SELECT  
    sum(actual_amount)
FROM 
    operating_budget
WHERE 
    fiscal_year = '2022'
    AND program_code = '031';