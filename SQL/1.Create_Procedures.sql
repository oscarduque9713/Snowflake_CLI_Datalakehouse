
USE ROLE DEVELOPER;
USE WAREHOUSE PROJECT;
USE DATABASE <% DB_NAME %>;

CREATE PROCEDURE IF NOT EXISTS BRONZE.SP_START_BATCH()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    V_BATCH_ID STRING;
BEGIN
    V_BATCH_ID := 'BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

    INSERT INTO BRONZE.PIPELINE_BATCH_CONTROL (
        BATCH_ID,
        STATUS,
        START_TS
    )
    VALUES (
        :V_BATCH_ID,
        'RUNNING',
        CURRENT_TIMESTAMP()
    );

    RETURN V_BATCH_ID;
END;
$$;


---- INGEST --

CREATE PROCEDURE IF NOT EXISTS BRONZE.SP_INGEST_RAW_FILES(P_BATCH_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

    COPY INTO BRONZE.CSV_RAW_CONTENT
    FROM (
        SELECT 
            $1 AS RAW_LINE,
            METADATA$FILENAME AS FILE_NAME,
            METADATA$FILE_ROW_NUMBER AS FILE_ROW_NUMBER,
            :P_BATCH_ID AS BATCH_ID
        FROM @BRONZE.RAW
    )
    FILE_FORMAT = (FORMAT_NAME = BRONZE.CSV)
    PATTERN = '(?i).*\.csv.*'
    ON_ERROR = CONTINUE;


    COPY INTO BRONZE.RAW_JSON_INGESTION
    FROM (
        SELECT 
            $1 AS JSON_CONTENT,
            METADATA$FILENAME AS FILE_NAME,
            METADATA$FILE_ROW_NUMBER AS FILE_ROW_NUMBER,
            CURRENT_TIMESTAMP() AS LOAD_DT,
            :P_BATCH_ID AS BATCH_ID
        FROM @BRONZE.RAW
    )
    FILE_FORMAT = (FORMAT_NAME = BRONZE.SEMITRUCTURED_DATA)
    PATTERN = '(?i).*\.json.*'
    ON_ERROR = CONTINUE;


    COPY INTO BRONZE.RAW_XML_INGESTION
    FROM (
        SELECT 
            $1 AS XML_CONTENT,
            METADATA$FILENAME AS FILE_NAME,
            METADATA$FILE_ROW_NUMBER AS FILE_ROW_NUMBER,
            CURRENT_TIMESTAMP() AS LOAD_DT,
            :P_BATCH_ID AS BATCH_ID
        FROM @BRONZE.RAW
    )
    FILE_FORMAT = (FORMAT_NAME = BRONZE.SEMITRUCTURED_DATA)
    PATTERN = '(?i).*\.(xml|txt).*'
    ON_ERROR = CONTINUE;

    RETURN 'Ingestion completed for batch: ' || P_BATCH_ID;

END;
$$;


----- VALIDATE BATCH LOAD 

CREATE OR REPLACE PROCEDURE BRONZE.SP_VALIDATE_BATCH_LOAD(P_BATCH_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    V_TOTAL_ROWS NUMBER DEFAULT 0;
    NO_ROWS_LOADED EXCEPTION (-20001, 'Pipeline stopped: no records were loaded for this batch.');
BEGIN

    SELECT COALESCE(TOTAL_ROWS, 0)
    INTO V_TOTAL_ROWS
    FROM BRONZE.PIPELINE_BATCH_CONTROL
    WHERE BATCH_ID = :P_BATCH_ID;

    IF (V_TOTAL_ROWS = 0) THEN
        RAISE NO_ROWS_LOADED;
    END IF;

    RETURN 'OK: Batch ' || P_BATCH_ID || ' has ' || V_TOTAL_ROWS || ' records loaded.';

END;
$$;

----- Validate Ingest------------------------

CREATE OR REPLACE PROCEDURE BRONZE.SP_UPDATE_BATCH_COUNTS(P_BATCH_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    V_CSV_ROWS NUMBER DEFAULT 0;
    V_JSON_ROWS NUMBER DEFAULT 0;
    V_XML_ROWS NUMBER DEFAULT 0;
    V_TOTAL_ROWS NUMBER DEFAULT 0;
BEGIN

    SELECT COUNT(*)
    INTO V_CSV_ROWS
    FROM BRONZE.CSV_RAW_CONTENT
    WHERE BATCH_ID = :P_BATCH_ID;

    SELECT COUNT(*)
    INTO V_JSON_ROWS
    FROM BRONZE.RAW_JSON_INGESTION
    WHERE BATCH_ID = :P_BATCH_ID;

    SELECT COUNT(*)
    INTO V_XML_ROWS
    FROM BRONZE.RAW_XML_INGESTION
    WHERE BATCH_ID = :P_BATCH_ID;

    V_TOTAL_ROWS := V_CSV_ROWS + V_JSON_ROWS + V_XML_ROWS;

    UPDATE BRONZE.PIPELINE_BATCH_CONTROL
    SET
        CSV_ROWS = :V_CSV_ROWS,
        JSON_ROWS = :V_JSON_ROWS,
        XML_ROWS = :V_XML_ROWS,
        TOTAL_ROWS = :V_TOTAL_ROWS,
        STATUS = 'INGESTED',
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE BATCH_ID = :P_BATCH_ID;

    RETURN 
        'Batch counts updated successfully. Batch ID: ' || P_BATCH_ID ||
        ', CSV rows: ' || V_CSV_ROWS ||
        ', JSON rows: ' || V_JSON_ROWS ||
        ', XML rows: ' || V_XML_ROWS ||
        ', Total rows: ' || V_TOTAL_ROWS;

END;
$$;

CREATE OR REPLACE PROCEDURE SILVER.SP_LOAD_SILVER_DIMENSIONS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

    ---- Dim customers

    INSERT INTO SILVER.DIM_CUSTOMERS
    SELECT CUSTOMER_ID,
        CUSTOMER_NAME,
        EMAIL,
        SEGMENT,
        'UNKNOWN' AS LOYALTY_TIER,
        'UNKNOWN' AS SIGNUP_SOURCE,
        IS_ACTIVE
    FROM SILVER.TRANSFROM_IN_CUSTOMER_CB
    UNION
    SELECT CUSTOMER_ID,
        CUSTOMER_NAME,
        EMAIL,
        'UNKNOWN' AS SEGMENT,
        LOYALTY_TIER,
        SIGNUP_SOURCE,
        IS_ACTIVE
    FROM SILVER.TRANSFROM_IN_CUSTOMER_CA;

/* ============================================================
   03 - PRODUCT, ORDER AND PAYMENT DIMENSIONS
   ============================================================ */

---- two files have the same format and columns, the duplicate values are removed and the anomalys that indicates in is_active col are removed, sku-0-999 and C-SKU-999

    INSERT INTO SILVER.DIM_PRODUCT
    SELECT *
    FROM (
        WITH AGGREGATED_INITIAL_PRODUCTS AS (
            SELECT 
                FILE_NAME,
                FILE_ROW_NUMBER,
                MAX(CASE WHEN KEY = 'col1' THEN VALUE END) AS SKU,
                MAX(CASE WHEN KEY = 'col2' THEN VALUE END) AS PRODUCT_NAME,
                MAX(CASE WHEN KEY = 'col3' THEN VALUE END) AS CATEGORY,
                MAX(CASE WHEN KEY = 'col4' THEN VALUE END)::FLOAT AS UNIT_PRICE,
                MAX(CASE WHEN KEY = 'col5' THEN VALUE END) AS CURRENCY,
                MAX(CASE WHEN KEY = 'col6' THEN VALUE END) AS ISACTIVE
            FROM SILVER.DIN_CSV_COLS
            WHERE FILE_NAME LIKE '%Product%.csv%'
              AND FILE_ROW_NUMBER <> 2
            GROUP BY FILE_NAME, FILE_ROW_NUMBER
        )
        SELECT 
            SKU,
            PRODUCT_NAME,
            CATEGORY,
            UNIT_PRICE,
            CURRENCY,
            SPLIT_PART(ISACTIVE, ' ', 1) AS IS_ACTIVE
        FROM AGGREGATED_INITIAL_PRODUCTS
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY SKU 
            ORDER BY FILE_ROW_NUMBER DESC, PRODUCT_NAME NULLS LAST
        ) = 1
        AND ISACTIVE NOT LIKE '%anomaly%'
    );

    ------------------ DIM ORDERS -------------------------------------------------------

--al tener dos archivos con solo una columna adicional en uno y el mismo orden se trabaja en un solo

    INSERT INTO SILVER.DIM_ORDER
    SELECT *
    FROM (
        WITH AGGREGATED_INITIAL_ORDER_A AS (
            SELECT 
                FILE_NAME,
                FILE_ROW_NUMBER,
                MAX(CASE WHEN KEY = 'col1' THEN VALUE END) AS ORDER_ID,
                MAX(CASE WHEN KEY = 'col2' THEN VALUE END) AS CUSTOMER_ID,
                MAX(CASE WHEN KEY = 'col3' THEN VALUE END)::DATE AS ORDER_DATE,
                MAX(CASE WHEN KEY = 'col4' THEN VALUE END) AS ORDER_STATUS_PRE,
                IFNULL(MAX(CASE WHEN KEY = 'col5' THEN VALUE END), 'UNKNOWN') AS CHANNEL_PRE
            FROM SILVER.DIN_CSV_COLS
            WHERE FILE_NAME LIKE '%Order%.csv%'
              AND FILE_ROW_NUMBER <> 2
            GROUP BY FILE_NAME, FILE_ROW_NUMBER
        )
        SELECT 
            ORDER_ID,
            CUSTOMER_ID,
            ORDER_DATE,
            SPLIT_PART(ORDER_STATUS_PRE, ' ', 1) AS ORDER_STATUS,
            SPLIT_PART(CHANNEL_PRE, ' ', 1) AS CHANNEL
        FROM AGGREGATED_INITIAL_ORDER_A
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ORDER_ID 
            ORDER BY FILE_ROW_NUMBER DESC
        ) = 1
        AND (
            ORDER_STATUS_PRE NOT LIKE '%invalid customer'
            AND CHANNEL_PRE NOT LIKE '%invalid customer'
        )
    );

    ---- DIM PAYMENTS ------------------------------------

    INSERT INTO SILVER.DIM_PAYMENTS
    SELECT *
    FROM (
        WITH AGGREGATED_INITIAL_PAYMENT AS (
            SELECT 
                FILE_NAME,
                FILE_ROW_NUMBER,
                MAX(CASE WHEN KEY = 'col1' THEN VALUE END) AS PAYMENT_ID,
                MAX(CASE WHEN KEY = 'col2' THEN VALUE END) AS ORDER_ID,
                MAX(CASE WHEN KEY = 'col3' THEN VALUE END) AS PAYMENT_METHOD,
                MAX(CASE WHEN KEY = 'col4' THEN VALUE END)::FLOAT AS AMOUNT,
                MAX(CASE WHEN KEY = 'col5' THEN VALUE END) AS CURRENCY,
                MAX(CASE WHEN KEY = 'col6' THEN VALUE END) AS STATUS
            FROM SILVER.DIN_CSV_COLS
            WHERE FILE_NAME LIKE '%Payments.csv%'
              AND FILE_ROW_NUMBER <> 2
            GROUP BY FILE_NAME, FILE_ROW_NUMBER
        )
        SELECT 
            PAYMENT_ID,
            ORDER_ID,
            PAYMENT_METHOD,
            CASE 
                WHEN STATUS LIKE 'REFUNDED%' THEN ABS(AMOUNT)
                ELSE AMOUNT
            END AS AMOUNT,
            CURRENCY,
            SPLIT_PART(STATUS, ' ', 1) AS STATUS
        FROM AGGREGATED_INITIAL_PAYMENT
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY PAYMENT_ID 
            ORDER BY FILE_ROW_NUMBER DESC
        ) = 1
    );

    RETURN 'Silver dimension tables loaded successfully.';

END;
$$;

----- SP 8 Busiisness rules to enrich information

CREATE OR REPLACE PROCEDURE SILVER.SP_APPLY_REFERENTIAL_RULES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

    /* ============================================================
       Referential consistency: DIM_CUSTOMERS
       ============================================================ */

    MERGE INTO SILVER.DIM_CUSTOMERS AS TGT
    USING (
        SELECT 
            'UNKNOWN' AS CUSTOMER_ID, 
            'UNKNOWN' AS CUSTOMER_NAME,
            'UNKNOWN' AS EMAIL,
            'UNKNOWN' AS SEGMENT,
            'UNKNOWN' AS LOYALTY_TIER,
            'UNKNOWN' AS SIGNUP_SOURCE,
            'UNKNOWN' AS IS_ACTIVE

        UNION ALL

        SELECT DISTINCT 
            F.CUSTOMER_ID, 
            F.CUSTOMER_NAME,
            'UNKNOWN' AS EMAIL,
            'UNKNOWN' AS SEGMENT,
            'UNKNOWN' AS LOYALTY_TIER,
            'UNKNOWN' AS SIGNUP_SOURCE,
            'UNKNOWN' AS IS_ACTIVE
        FROM SILVER.SLV_TRANSACTIONS F
        WHERE F.CUSTOMER_ID <> 'UNKNOWN'
    ) AS SRC
    ON TGT.CUSTOMER_ID = SRC.CUSTOMER_ID
    WHEN NOT MATCHED THEN
        INSERT (
            CUSTOMER_ID,
            CUSTOMER_NAME,
            EMAIL,
            SEGMENT,
            LOYALTY_TIER,
            SIGNUP_SOURCE,
            IS_ACTIVE
        )
        VALUES (
            SRC.CUSTOMER_ID,
            SRC.CUSTOMER_NAME,
            SRC.EMAIL,
            SRC.SEGMENT,
            SRC.LOYALTY_TIER,
            SRC.SIGNUP_SOURCE,
            SRC.IS_ACTIVE
        );


    /* ============================================================
       Referential consistency: DIM_PRODUCT
       ============================================================ */

    MERGE INTO SILVER.DIM_PRODUCT AS TGT
    USING (
        SELECT 
            'UNKNOWN' AS SKU, 
            'UNKNOWN' AS PRODUCT_NAME,
            'UNKNOWN' AS CATEGORY,
            0 AS UNIT_PRICE,
            'UNKNOWN' AS CURRENCY,
            'UNKNOWN' AS IS_ACTIVE

        UNION ALL

        SELECT DISTINCT 
            F.SKU, 
            F.DESCRIPTION AS PRODUCT_NAME,
            'UNKNOWN' AS CATEGORY,
            NULL AS UNIT_PRICE,  
            'UNKNOWN' AS CURRENCY,
            'UNKNOWN' AS IS_ACTIVE
        FROM SILVER.SLV_TRANSACTIONS F
        WHERE F.SKU <> 'UNKNOWN'
    ) AS SRC
    ON TGT.SKU = SRC.SKU
    WHEN NOT MATCHED THEN
        INSERT (
            SKU,
            PRODUCT_NAME,
            CATEGORY,
            UNIT_PRICE,
            CURRENCY,
            IS_ACTIVE
        )
        VALUES (
            SRC.SKU,
            SRC.PRODUCT_NAME,
            SRC.CATEGORY,
            SRC.UNIT_PRICE,
            SRC.CURRENCY,
            SRC.IS_ACTIVE
        );


    /* ============================================================
       Referential consistency: DIM_PAYMENTS
       ============================================================ */

    MERGE INTO SILVER.DIM_PAYMENTS AS TGT
    USING (
        SELECT 
            'UNKNOWN' AS PAYMENT_ID, 
            'UNKNOWN' AS ORDER_ID,
            'UNKNOWN' AS PAYMENT_METHOD,
            0 AS AMOUNT,
            'UNKNOWN' AS CURRENCY,
            'UNKNOWN' AS STATUS

        UNION ALL

        SELECT 
            'UNKNOWN' AS PAYMENT_ID, 
            F.ORDER_ID,
            F.PAYMENT_METHOD,
            F.PAYMENT_AMOUNT AS AMOUNT,
            F.CURRENCY,
            'UNKNOWN' AS STATUS
        FROM SILVER.SLV_TRANSACTIONS F
        WHERE F.ORDER_ID <> 'UNKNOWN'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY F.ORDER_ID
            ORDER BY F.ORDER_DATE DESC
        ) = 1
    ) AS SRC
    ON TGT.ORDER_ID = SRC.ORDER_ID
    WHEN NOT MATCHED THEN
        INSERT (
            PAYMENT_ID,
            ORDER_ID,
            PAYMENT_METHOD,
            AMOUNT,
            CURRENCY,
            STATUS
        )
        VALUES (
            SRC.PAYMENT_ID,
            SRC.ORDER_ID,
            SRC.PAYMENT_METHOD,
            SRC.AMOUNT,
            SRC.CURRENCY,
            SRC.STATUS
        );


    /* ============================================================
       Referential consistency: DIM_ORDER
       ============================================================ */

    MERGE INTO SILVER.DIM_ORDER AS TGT
    USING (
        SELECT 
            'UNKNOWN' AS ORDER_ID,
            'UNKNOWN' AS CUSTOMER_ID,
            NULL AS ORDER_DATE,
            'UNKNOWN' AS ORDER_STATUS,
            'UNKNOWN' AS CHANNEL

        UNION ALL

        SELECT  
            F.ORDER_ID,
            F.CUSTOMER_ID,
            F.ORDER_DATE,
            'UNKNOWN' AS ORDER_STATUS,
            'UNKNOWN' AS CHANNEL
        FROM SILVER.SLV_TRANSACTIONS F
        WHERE F.ORDER_ID <> 'UNKNOWN'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY F.ORDER_ID
            ORDER BY F.ORDER_DATE DESC
        ) = 1
    ) AS SRC
    ON TGT.ORDER_ID = SRC.ORDER_ID
    WHEN NOT MATCHED THEN
        INSERT (
            ORDER_ID,
            CUSTOMER_ID,
            ORDER_DATE,
            ORDER_STATUS,
            CHANNEL
        )
        VALUES (
            SRC.ORDER_ID,
            SRC.CUSTOMER_ID,
            SRC.ORDER_DATE,
            SRC.ORDER_STATUS,
            SRC.CHANNEL
        );

    RETURN 'Referential consistency rules applied successfully.';

END;
$$;

----- 9 load gold

CREATE OR REPLACE PROCEDURE GOLD.SP_LOAD_GOLD_MODEL()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN



    /* ============================================================
       01 - MAIN TRANSACTION FACT
       ============================================================ */

    INSERT INTO GOLD.FACT_TRANSACTIONS (
        TRANSACTION_ID,
        ORDER_ID,
        ORDER_DATE,
        CUSTOMER_ID,
        SKU,
        QUANTITY,
        PAYMENT_AMOUNT
    )
    SELECT 
        TRANSACTION_ID,
        ORDER_ID,
        ORDER_DATE,
        CUSTOMER_ID,
        SKU,
        QUANTITY,
        PAYMENT_AMOUNT
    FROM SILVER.SLV_TRANSACTIONS;


    /* ============================================================
       02 - CUSTOMER DIMENSION
       ============================================================ */

    INSERT INTO GOLD.DIM_CUSTOMERS
    SELECT *
    FROM SILVER.DIM_CUSTOMERS;


    /* ============================================================
       03 - PRODUCT DIMENSION
       ============================================================ */

    INSERT INTO GOLD.DIM_PRODUCT
    SELECT *
    FROM SILVER.DIM_PRODUCT;


    /* ============================================================
       04 - ORDER DIMENSION
       ============================================================ */

    INSERT INTO GOLD.DIM_ORDER (
        ORDER_ID,
        ORDER_DATE,
        ORDER_STATUS,
        CHANNEL
    )
    SELECT 
        ORDER_ID,
        ORDER_DATE,
        ORDER_STATUS,
        CHANNEL
    FROM SILVER.DIM_ORDER;


    /* ============================================================
       05 - PAYMENT DIMENSION
       ============================================================ */

    INSERT INTO GOLD.DIM_PAYMENT (
        PAYMENT_ID,
        ORDER_ID,
        PAYMENT_METHOD,
        CURRENCY,
        STATUS
    )
    SELECT 
        PAYMENT_ID,
        ORDER_ID,
        PAYMENT_METHOD,
        CURRENCY,
        STATUS
    FROM SILVER.DIM_PAYMENTS;


    /* ============================================================
       06 - REJECTION / GAP TABLE
       Orders without related transactions
       ============================================================ */

    INSERT INTO GOLD.FACT_ORDER_GAPS (
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_DATE,
        GAP_TYPE,
        PAYMENT_AMOUNT
    )
    SELECT
        DO.ORDER_ID,
        DO.CUSTOMER_ID,
        DO.ORDER_DATE,
        'NO_TRANSACTION' AS GAP_TYPE,
        COALESCE(SUM(DP.AMOUNT), 0) AS PAYMENT_AMOUNT
    FROM SILVER.DIM_ORDER DO
    LEFT JOIN SILVER.SLV_TRANSACTIONS F
        ON DO.ORDER_ID = F.ORDER_ID
    LEFT JOIN SILVER.DIM_PAYMENTS DP
        ON DO.ORDER_ID = DP.ORDER_ID
    WHERE F.ORDER_ID IS NULL
    GROUP BY 
        DO.ORDER_ID,
        DO.CUSTOMER_ID,
        DO.ORDER_DATE;


    RETURN 'Gold model loaded successfully.';

END;
$$;

--- 10 End Batch

CREATE OR REPLACE PROCEDURE BRONZE.SP_END_BATCH_SUCCESS(P_BATCH_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    V_BATCH_EXISTS NUMBER DEFAULT 0;
    BATCH_NOT_FOUND EXCEPTION (-20003, 'Pipeline stopped: batch ID does not exist.');
BEGIN

    SELECT COUNT(*)
    INTO V_BATCH_EXISTS
    FROM BRONZE.PIPELINE_BATCH_CONTROL
    WHERE BATCH_ID = :P_BATCH_ID;

    IF (V_BATCH_EXISTS = 0) THEN
        RAISE BATCH_NOT_FOUND;
    END IF;

    UPDATE BRONZE.PIPELINE_BATCH_CONTROL
    SET
        STATUS = 'SUCCESS',
        END_TS = CURRENT_TIMESTAMP(),
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE BATCH_ID = :P_BATCH_ID;

    RETURN 'Batch completed successfully. Batch ID: ' || P_BATCH_ID;

END;
$$;
