-- QUERY 4
-- Find customers who spend more money via catalog than in stores. Identify preferred customers and their country of origin.

WITH catalog AS (
    SELECT
        c.C_CUSTOMER_SK,
        SUM(cs.CS_SALES_PRICE) AS total_catalog_spending
    FROM {{ source('tpcds', 'customer') }} c
    JOIN {{ source('tpcds', 'catalog_sales') }} cs ON c.C_CUSTOMER_SK = cs.CS_BILL_CUSTOMER_SK
    JOIN {{ source('tpcds', 'date_dim') }} d ON cs.CS_SOLD_DATE_SK = d.d_date_sk
    WHERE d.d_year = 2001
    GROUP BY
        c.C_CUSTOMER_SK
),

store AS (
    SELECT
        c.C_CUSTOMER_SK,
        SUM(ss.SS_NET_PAID) AS total_store_spending
    FROM {{ source('tpcds', 'customer') }} c
    JOIN {{ source('tpcds', 'store_sales') }} ss ON c.C_CUSTOMER_SK = ss.SS_CUSTOMER_SK
    JOIN {{ source('tpcds', 'date_dim') }} d ON ss.SS_SOLD_DATE_SK = d.d_date_sk
    WHERE d.d_year = 2001
    GROUP BY
        c.C_CUSTOMER_SK
),

customer_country AS (
    SELECT
        c.C_CUSTOMER_SK,
        ca.CA_COUNTRY
    FROM {{ source('tpcds', 'customer') }} c
    JOIN {{ source('tpcds', 'customer_address') }} ca ON c.C_CURRENT_ADDR_SK = CA.CA_ADDRESS_SK 
)

SELECT
    cc.C_CUSTOMER_SK,
    cc.CA_COUNTRY,
    COALESCE(cs.total_catalog_spending, 0) AS total_catalog_spending,
    COALESCE(ss.total_store_spending, 0) AS total_store_spending
FROM
    customer_country cc
LEFT JOIN
    catalog cs ON cc.C_CUSTOMER_SK = cs.C_CUSTOMER_SK
LEFT JOIN
    store ss ON cc.C_CUSTOMER_SK = ss.C_CUSTOMER_SK
WHERE
    cs.total_catalog_spending > ss.total_store_spending
LIMIT 5
