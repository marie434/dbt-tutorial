-- QUERY 10

WITH tab1 AS (
    SELECT
        c.C_CUSTOMER_SK AS customer_id,
        COUNT(DISTINCT CASE WHEN ss.SS_STORE_SK IS NOT NULL THEN 'store' END) AS store_purchases,
        COUNT(DISTINCT CASE WHEN cs.CS_SOLD_DATE_SK IS NOT NULL THEN 'catalog' END) AS catalog_purchases
    FROM {{ source('tpcds', 'customer') }} c
    LEFT JOIN {{ source('tpcds', 'store_sales') }} ss ON c.C_CUSTOMER_SK = ss.SS_CUSTOMER_SK
    LEFT JOIN {{ source('tpcds', 'catalog_sales') }} cs ON c.C_CUSTOMER_SK = cs.CS_BILL_CUSTOMER_SK
    JOIN {{ source('tpcds', 'customer_address') }} ca ca ON c.C_CURRENT_ADDR_SK = ca.ca_address_sk
    JOIN {{ source('tpcds', 'date_dim') }} d1 ON ss.SS_SOLD_DATE_SK = d1.d_date_sk
    JOIN {{ source('tpcds', 'date_dim') }} d2 ON cs.CS_SOLD_DATE_SK = d2.d_date_sk
    WHERE
        ca.CA_COUNTY IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County')
        AND d1.d_year = 2002 AND d2.d_year = 2002
    GROUP BY
        c.C_CUSTOMER_SK
)

SELECT
    COUNT(*) AS customer_count
FROM
    tab1 
WHERE
    tab1.store_purchases > 0
    AND tab1.catalog_purchases > 0