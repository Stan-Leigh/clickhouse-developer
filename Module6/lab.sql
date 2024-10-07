CREATE VIEW london_properties_view AS 
    SELECT
        date, 
        price, 
        addr1, 
        addr2,
        street
    FROM
        uk_price_paid
    WHERE town = 'LONDON';

SELECT
    avg(price)
FROM
    london_properties_view
WHERE
    toYear(date) = '2022';

SELECT
    count()
FROM
    london_properties_view;

SELECT count() 
FROM uk_price_paid
WHERE town = 'LONDON';

EXPLAIN SELECT count() 
FROM london_properties_view;

EXPLAIN SELECT count() 
FROM uk_price_paid
WHERE town = 'LONDON';

CREATE VIEW properties_by_town_view AS
    SELECT
        date,
        price,
        addr1,
        addr2,
        street
    FROM 
        uk_price_paid
    WHERE 
        town = {town_filter:String};

SELECT
    max(price),
    argMax(street, price)
FROM 
    properties_by_town_view(town_filter='LIVERPOOL');


-- Lab 6.2
SELECT
    count(),
    avg(price)
FROM 
    uk_price_paid
WHERE 
    toYear(date) = '2020';

SELECT
    toYear(date),
    count(),
    avg(price)
FROM 
    uk_price_paid
GROUP BY 
    toYear(date)
ORDER BY 
    toYear(date) ASC;

CREATE TABLE prices_by_year_dest (
    price UInt32,
    date Date,
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
PRIMARY KEY (town, date)
PARTITION BY toYear(date);

CREATE MATERIALIZED VIEW prices_by_year_view
TO prices_by_year_dest
AS
    SELECT
        price,
        date,
        addr1,
        addr2,
        street,
        town,
        district,
        county
    FROM uk_price_paid;

INSERT INTO prices_by_year_dest
    SELECT
        price,
        date,
        addr1,
        addr2,
        street,
        town,
        district,
        county
    FROM uk_price_paid;

SELECT 
    count()
FROM 
    prices_by_year_dest;

SELECT * FROM system.parts
WHERE table='prices_by_year_dest';

SELECT * FROM system.parts
WHERE table='uk_price_paid';

SELECT
    count(),
    avg(price)
FROM 
    prices_by_year_dest
WHERE 
    toYear(date) = '2020';

SELECT
    count(),
    max(price),
    avg(price),
    quantile(0.90)(price)
FROM 
    prices_by_year_dest
WHERE 
    county = 'STAFFORDSHIRE'
    AND date >= toDate('2005-06-01') AND date <= toDate('2005-06-30');

INSERT INTO uk_price_paid VALUES
    (125000, '2024-03-07', 'B77', '4JT', 'semi-detached', 0, 'freehold', 10,'',	'CRIGDON','WILNECOTE','TAMWORTH','TAMWORTH','STAFFORDSHIRE'),
    (440000000, '2024-07-29', 'WC1B', '4JB', 'other', 0, 'freehold', 'VICTORIA HOUSE', '', 'SOUTHAMPTON ROW', '','LONDON','CAMDEN', 'GREATER LONDON'),
    (2000000, '2024-01-22','BS40', '5QL', 'detached', 0, 'freehold', 'WEBBSBROOK HOUSE','', 'SILVER STREET', 'WRINGTON', 'BRISTOL', 'NORTH SOMERSET', 'NORTH SOMERSET');

SELECT 
    * 
FROM 
    prices_by_year_dest
WHERE 
    toYear(date) = '2024';

SELECT * FROM system.parts
WHERE table='prices_by_year_dest';