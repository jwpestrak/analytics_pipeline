-- How to add index to temporary table?

CREATE TEMPORARY TABLE IF NOT EXISTS SAMPLE_CUST_KEY AS (
    SELECT 
        A.TRAN_KEY
        , B.CUST_KEY
    FROM PH_CUST_TRAN_ASGNMT_DIM AS A
    RIGHT JOIN (
        SELECT DISTINCT
            CUST_KEY
        FROM PH_CUST_TRAN_ASGNMT_DIM
        WHERE EMPLY_REL_FL <> 'Y'
        ORDER BY RAND() 
        LIMIT 10000
    ) AS B
    ON (A.CUST_KEY = B.CUST_KEY)
)
;
