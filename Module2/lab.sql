SELECT 
    *
FROM
    s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet')
LIMIT
    10;

DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

SELECT 
    count()
FROM
    s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

CREATE TABLE pypi (
    TIMESTAMP DateTime64,
    COUNTRY_CODE String,
    URL String,
    PROJECT String
)
ENGINE = MergeTree
PRIMARY KEY TIMESTAMP;

INSERT INTO pypi
    SELECT TIMESTAMP, COUNTRY_CODE, URL, PROJECT
    FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/pypi/2023/pypi_0_7_34.snappy.parquet');

SELECT
    PROJECT,
    count(PROJECT) AS total_projects
FROM
    pypi
GROUP BY
    PROJECT
ORDER BY 
    total_projects DESC
LIMIT 100;

SELECT
    PROJECT,
    count(PROJECT) AS total_projects
FROM
    pypi
WHERE
    toStartOfMonth(TIMESTAMP) = '2023-04-01'
GROUP BY
    PROJECT
ORDER BY 
    total_projects DESC
LIMIT 100;

SELECT
    PROJECT,
    count(PROJECT) AS total_projects
FROM
    pypi
WHERE
    PROJECT LIKE 'boto%'
GROUP BY
    PROJECT
ORDER BY 
    total_projects DESC
LIMIT 100;

CREATE TABLE pypi2 (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String 
)
ENGINE = MergeTree
PRIMARY KEY (TIMESTAMP, PROJECT);

INSERT INTO pypi2
    SELECT *
    FROM pypi;

SELECT 
    PROJECT,
    count() AS c
FROM pypi2
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

CREATE OR REPLACE TABLE pypi2 (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String 
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, TIMESTAMP);

INSERT INTO pypi2
    SELECT *
    FROM pypi;

SELECT 
    PROJECT,
    count() AS c
FROM pypi2
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;


-- LAB 2.2
SELECT
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table = 'pypi');

SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table LIKE '%pypi%')
GROUP BY table;

CREATE TABLE test_pypi (
    TIMESTAMP DateTime64,
    COUNTRY_CODE String,
    URL String,
    PROJECT String
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, COUNTRY_CODE, TIMESTAMP);

INSERT INTO test_pypi
    SELECT * FROM pypi2;

SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM 
    system.parts
WHERE 
    (active = 1) AND (table LIKE '%pypi%')
GROUP BY 
    table;