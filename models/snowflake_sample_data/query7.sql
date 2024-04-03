
WITH SALES AS (
    SELECT
        ss.SS_QUANTITY,
        ss.SS_LIST_PRICE,
        ss.SS_WHOLESALE_COST,
        ss.SS_SALES_PRICE,
        ss.SS_EXT_DISCOUNT_AMT
    FROM {{ source('tpcds', 'store_sales') }} ss
    JOIN {{ source('tpcds', 'promotion') }} p ON ss.SS_PROMO_SK = p.P_PROMO_SK
    JOIN {{ source('tpcds', 'customer_demographics') }} cd ON ss.SS_CUSTOMER_SK = cd.CD_DEMO_SK
    JOIN {{ source('tpcds', 'store') }} s ON ss.SS_STORE_SK = s.S_STORE_SK
    JOIN {{ source('tpcds', 'date_dim') }} d ON ss.SS_SOLD_DATE_SK = d.d_date_sk    
    WHERE
        p.P_CHANNEL_DMAIL != 'Y' 
        AND p.P_CHANNEL_DEMO != 'Y' 
        AND d.d_year = 2000 
        AND cd.CD_EDUCATION_STATUS = 'College' 
        AND cd.CD_MARITAL_STATUS = 'S' 
        AND cd.CD_GENDER = 'M'
)
SELECT
    AVG(SS_QUANTITY) AS average_quantity,
    AVG(SS_LIST_PRICE) AS average_list_price,
    AVG(SS_EXT_DISCOUNT_AMT) AS average_discount_amount,
    AVG(SS_SALES_PRICE) AS average_sales_price
FROM
    sales
