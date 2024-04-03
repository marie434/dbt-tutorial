-- QUERY 3
-- Report the total extended sales price per item brand of a specific manufacturer for all sales in a specific month of the year. 

WITH tab1 AS (
    SELECT
        i.I_ITEM_SK,
        i.I_BRAND,
        ss.SS_EXT_SALES_PRICE
    FROM {{ source('tpcds', 'store_sales') }} ss
    JOIN {{ source('tpcds', 'item') }} i ON ss.SS_ITEM_SK = i.I_ITEM_SK
    JOIN {{ source('tpcds', 'date_dim') }} d ON ss.SS_SOLD_DATE_SK = d.d_date_sk
    WHERE d_moy = 11 AND i.i_manufact_id = 128 
)

SELECT
    I_BRAND,
    SUM(SS_EXT_SALES_PRICE) AS total_ext_sales_price
FROM tab1
GROUP BY I_BRAND
ORDER BY total_ext_sales_price
limit 10
