-- Big-table load generator for Snowflake (database: RECON_DB, schema: PUBLIC)
-- Mirrors db/demo/oracle_big_table_setup.sql's DEMO_BIG_TXN table, but injects
-- controlled, predictable differences so a reconciliation run produces a
-- known mix of HASH_MISMATCH / MISSING_IN_DEST / MISSING_IN_SOURCE diffs at scale.
--
-- Run with SnowSQL:
--   snowsql -a <account> -u <user> -f db/demo/snowflake_big_table_setup.sql
--
-- Uses TABLE(GENERATOR(...)) which is Snowflake's fastest way to bulk-generate
-- rows (millions of rows in seconds, no client-side round trips).

USE DATABASE RECON_DB;
USE SCHEMA PUBLIC;

DROP TABLE IF EXISTS DEMO_BIG_TXN;

CREATE TABLE DEMO_BIG_TXN (
    TXN_ID            NUMBER(18,0)   NOT NULL,
    CUSTOMER_ID       NUMBER(12,0)   NOT NULL,
    ACCOUNT_ID        NUMBER(12,0)   NOT NULL,
    AMOUNT            NUMBER(14,2)   NOT NULL,
    CURRENCY          STRING         NOT NULL,
    TXN_STATUS        STRING         NOT NULL,
    TXN_TYPE          STRING         NOT NULL,
    DESCRIPTION       STRING,
    TXN_TS            TIMESTAMP_NTZ  NOT NULL,
    LAST_UPDATED_TS   TIMESTAMP_NTZ  NOT NULL,
    CONSTRAINT PK_DEMO_BIG_TXN PRIMARY KEY (TXN_ID)
);

-- ═══════════════════════════════════════════════════════════════════════
-- Tunables — MUST match the Oracle script's TOTAL_ROWS for the offsets
-- below to line up correctly.
-- ═══════════════════════════════════════════════════════════════════════
SET TOTAL_ROWS      = 120000;  -- same as Oracle TOTAL_ROWS
SET MISSING_TAIL     = 1200;   -- highest N Oracle TXN_IDs are skipped here -> MISSING_IN_DEST
SET MISMATCH_EVERY   = 137;    -- every Nth row gets a different AMOUNT -> HASH_MISMATCH
SET DEST_ONLY_ROWS   = 500;    -- extra Snowflake-only rows -> MISSING_IN_SOURCE

-- 1) Bulk-load TOTAL_ROWS - MISSING_TAIL rows mirroring Oracle IDs 1..N,
--    with a deterministic AMOUNT change every MISMATCH_EVERY rows.
INSERT INTO DEMO_BIG_TXN (TXN_ID, CUSTOMER_ID, ACCOUNT_ID, AMOUNT, CURRENCY,
                           TXN_STATUS, TXN_TYPE, DESCRIPTION, TXN_TS, LAST_UPDATED_TS)
SELECT
    n,
    1000 + MOD(n, 200000),
    5000 + MOD(n, 50000),
    CASE WHEN MOD(n, $MISMATCH_EVERY) = 0
         THEN ROUND(UNIFORM(1, 25000, RANDOM()) + 500, 2)   -- deliberately different -> HASH_MISMATCH
         ELSE ROUND(UNIFORM(1, 25000, RANDOM()), 2)
    END AS amount,
    ARRAY_CONSTRUCT('USD','EUR','GBP','BRL','JPY')[MOD(n, 5)]::STRING,
    ARRAY_CONSTRUCT('POSTED','PENDING','FAILED','REVERSED','SETTLED')[MOD(n, 5)]::STRING,
    ARRAY_CONSTRUCT('DEBIT','CREDIT','TRANSFER','FEE','REFUND')[MOD(n, 5)]::STRING,
    'Auto-generated txn #' || n,
    DATEADD(SECOND, MOD(n, 500000), TIMESTAMP '2026-01-01 00:00:00'),
    DATEADD(SECOND, MOD(n, 500000), TIMESTAMP '2026-01-01 00:00:00')
FROM (
    SELECT SEQ4() + 1 AS n
    FROM TABLE(GENERATOR(ROWCOUNT => $TOTAL_ROWS - $MISSING_TAIL))
);

-- 2) Extra dest-only rows with IDs beyond anything Oracle generated
--    -> these will show up as MISSING_IN_SOURCE.
INSERT INTO DEMO_BIG_TXN (TXN_ID, CUSTOMER_ID, ACCOUNT_ID, AMOUNT, CURRENCY,
                           TXN_STATUS, TXN_TYPE, DESCRIPTION, TXN_TS, LAST_UPDATED_TS)
SELECT
    $TOTAL_ROWS + rn AS txn_id,
    1000 + MOD(rn, 200000),
    5000 + MOD(rn, 50000),
    ROUND(UNIFORM(1, 25000, RANDOM()), 2),
    'USD', 'POSTED', 'DEBIT',
    'Dest-only txn #' || ($TOTAL_ROWS + rn),
    DATEADD(SECOND, MOD(rn, 500000), TIMESTAMP '2026-01-01 00:00:00'),
    DATEADD(SECOND, MOD(rn, 500000), TIMESTAMP '2026-01-01 00:00:00')
FROM (
    SELECT SEQ4() + 1 AS rn
    FROM TABLE(GENERATOR(ROWCOUNT => $DEST_ONLY_ROWS))
);

SELECT COUNT(*) AS row_count FROM DEMO_BIG_TXN;

