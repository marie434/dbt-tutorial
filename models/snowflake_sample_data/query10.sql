-- QUERY 10
-- Count the customers with the same gender, marital status, education status, purchase estimate, credit rating,
-- dependent count, employed dependent count and college dependent count who live in certain counties and who
-- have purchased from both stores and another sales channel during a three month time period of a given year. 

WITH tab1 AS (
    SELECT
        c.C_CUSTOMER_SK,
        cd.CD_GENDER,
        cd.CD_MARITAL_STATUS,
        cd.CD_EDUCATION_STATUS,
        cd.CD_PURCHASE_ESTIMATE,
        cd.CD_CREDIT_RATING,
        cd.CD_DEP_COUNT,
        cd.CD_DEP_EMPLOYED_COUNT,
        cd.CD_DEP_COLLEGE_COUNT,
        COUNT(DISTINCT CASE WHEN ss.SS_STORE_SK IS NOT NULL THEN 'store' END) AS store_purchases,
        COUNT(DISTINCT CASE WHEN cs.CS_SOLD_DATE_SK IS NOT NULL THEN 'catalog' END) AS catalog_purchases
    FROM {{ source('tpcds', 'customer') }} c
    JOIN {{ source('tpcds', 'customer_demographics') }} cd ON c.C_CURRENT_CDEMO_SK = cd.CD_DEMO_SK
    LEFT JOIN {{ source('tpcds', 'store_sales') }} ss ON c.C_CUSTOMER_SK = ss.SS_CUSTOMER_SK
    LEFT JOIN {{ source('tpcds', 'catalog_sales') }} cs ON c.C_CUSTOMER_SK = cs.CS_BILL_CUSTOMER_SK
    JOIN {{ source('tpcds', 'customer_address') }} ca ON c.C_CURRENT_ADDR_SK = ca.ca_address_sk
    JOIN {{ source('tpcds', 'date_dim') }} d1 ON ss.SS_SOLD_DATE_SK = d1.d_date_sk
    JOIN {{ source('tpcds', 'date_dim') }} d2 ON cs.CS_SOLD_DATE_SK = d2.d_date_sk
    WHERE
        ca.CA_COUNTY IN ('Rush County', 'Toole County', 'Jefferson County', 'Dona Ana County', 'La Porte County')
        AND d1.d_year = 2002 AND d2.d_year = 2002 AND d1.d_moy BETWEEN 1 AND 3 AND d2.d_moy BETWEEN 1 AND 3
    GROUP BY
        c.C_CUSTOMER_SK, cd.CD_GENDER, cd.CD_MARITAL_STATUS, cd.CD_EDUCATION_STATUS, cd.CD_PURCHASE_ESTIMATE, cd.CD_CREDIT_RATING, cd.CD_DEP_COUNT, cd.CD_DEP_EMPLOYED_COUNT, cd.CD_DEP_COLLEGE_COUNT
)

SELECT
    COUNT(*) AS customer_count,
    CD_GENDER,
    CD_MARITAL_STATUS,
    CD_EDUCATION_STATUS,
    CD_PURCHASE_ESTIMATE,
    CD_CREDIT_RATING,
    CD_DEP_COUNT,
    CD_DEP_EMPLOYED_COUNT,
    CD_DEP_COLLEGE_COUNT
FROM
    tab1 
WHERE
    tab1.store_purchases > 0
    AND tab1.catalog_purchases > 0
GROUP BY
    CD_GENDER,
    CD_MARITAL_STATUS,
    CD_EDUCATION_STATUS,
    CD_PURCHASE_ESTIMATE,
    CD_CREDIT_RATING,
    CD_DEP_COUNT,
    CD_DEP_EMPLOYED_COUNT,
    CD_DEP_COLLEGE_COUNT
ORDER BY
    CD_GENDER,
    CD_MARITAL_STATUS,
    CD_EDUCATION_STATUS,
    CD_PURCHASE_ESTIMATE,
    CD_CREDIT_RATING,
    CD_DEP_COUNT,
    CD_DEP_EMPLOYED_COUNT,
    CD_DEP_COLLEGE_COUNT,
    COUNT(*) desc

--LIMIT 10
