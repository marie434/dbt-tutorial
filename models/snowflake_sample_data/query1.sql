
WITH store_avg_returns AS (
    SELECT
        sr.SR_STORE_SK AS store_id,
        AVG(sr.SR_RETURN_AMT) AS store_avg_return_amount
    FROM {{ source('tpcds', 'store_returns') }} sr
    JOIN {{ source('tpcds', 'store') }} s ON sr.SR_STORE_SK = s.S_STORE_SK
    JOIN {{ source('tpcds', 'date_dim') }} d ON sr.SR_RETURNED_DATE_SK = d.d_date_sk
    WHERE
        d.d_year = 2000
        AND s.S_STATE = 'TN'
    GROUP BY
        sr.SR_STORE_SK
),

customer_return_totals AS (
    SELECT
        sr.SR_CUSTOMER_SK AS customer_id,
        sr.SR_STORE_SK AS store_id,
        SUM(sr.SR_RETURN_AMT) AS customer_total_return_amount
    FROM {{ source('tpcds', 'store_returns') }} sr
    JOIN {{ source('tpcds', 'store') }} s ON sr.SR_STORE_SK = s.S_STORE_SK
    JOIN {{ source('tpcds', 'date_dim') }} d ON sr.SR_RETURNED_DATE_SK = d.d_date_sk
    WHERE
        d.d_year = 2000
        AND s.S_STATE = 'TN'
    GROUP BY
        sr.SR_CUSTOMER_SK,
        sr.SR_STORE_SK
)

SELECT
    crt.customer_id,
    crt.store_id,
    crt.customer_total_return_amount,
    sar.store_avg_return_amount,
    (crt.customer_total_return_amount - sar.store_avg_return_amount) / sar.store_avg_return_amount AS return_difference_percentage
FROM
    customer_return_totals crt
JOIN
    store_avg_returns sar ON crt.store_id = sar.store_id
WHERE
    (crt.customer_total_return_amount - sar.store_avg_return_amount) / sar.store_avg_return_amount > 0.2;
