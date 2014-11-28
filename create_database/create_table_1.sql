-- Add partition for BUS_DT

CREATE TABLE IF NOT EXISTS SLS_CUST_TRN_DTL_FCT (
    TRAN_KEY INT UNSIGNED NOT NULL,
    TRAN_LINE_KEY TINYINT UNSIGNED NOT NULL,
    CUST_KEY MEDIUMINT UNSIGNED,
    CUST_AGE TINYINT UNSIGNED,
    CUST_GNDR CHAR(1),
    ORDER_NUM MEDIUMINT UNSIGNED,
    ORDER_TYP_CD VARCHAR(20) NOT NULL,
    UPC_IDNT CHAR(13),
    TRAN_LINE_NET_AMT DECIMAL(7, 2) DEFAULT 0.00,
    TRAN_LINE_ORIGINAL_AMT DECIMAL(7, 2) DEFAULT 0.00,
    BUS_DT DATE NOT NULL, 
    PRIMARY KEY (TRAN_KEY, TRAN_LINE_KEY)
) ENGINE=InnoDB
;