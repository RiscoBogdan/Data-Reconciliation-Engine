-- Big-table load generator for Oracle (schema: RECON)
-- Purpose: create a large DEMO_BIG_TXN table with a configurable row count so you
-- can stress-test the reconciler engine's batching, hashing, and UI at scale.
--
-- Run with SQL*Plus / SQLcl:
--   sqlplus RECON_APP/<pwd>@FREEPDB1 @db/demo/oracle_big_table_setup.sql
--
-- NOTE: if running via a plain JDBC console (IntelliJ/DataGrip DB console,
-- DBeaver, etc.) instead of sqlplus/SQLcl, remove or don't run the
-- "SET SERVEROUTPUT ON" / "SET TIMING ON" lines below — those are
-- SQL*Plus-only client commands, not real SQL, and Oracle will reject them
-- with ORA-00922 (missing or invalid option) if sent directly over JDBC.
--
-- Tune TOTAL_ROWS / BATCH_SIZE below before running. Inserts commit every
-- BATCH_SIZE rows to keep UNDO usage bounded no matter how large TOTAL_ROWS is.

ALTER SESSION SET CURRENT_SCHEMA = RECON;

-- SQL*Plus/SQLcl only — comment out (or skip when pasting into a JDBC console):
-- SET SERVEROUTPUT ON
-- SET TIMING ON

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE DEMO_BIG_TXN PURGE';
EXCEPTION WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

CREATE TABLE DEMO_BIG_TXN (
    TXN_ID            NUMBER(18)      NOT NULL,
    CUSTOMER_ID       NUMBER(12)      NOT NULL,
    ACCOUNT_ID        NUMBER(12)      NOT NULL,
    AMOUNT            NUMBER(14,2)    NOT NULL,
    CURRENCY          VARCHAR2(3)     NOT NULL,
    TXN_STATUS        VARCHAR2(20)    NOT NULL,
    TXN_TYPE          VARCHAR2(20)    NOT NULL,
    DESCRIPTION       VARCHAR2(200),
    TXN_TS            TIMESTAMP       NOT NULL,
    LAST_UPDATED_TS   TIMESTAMP       NOT NULL,
    CONSTRAINT PK_DEMO_BIG_TXN PRIMARY KEY (TXN_ID)
);

CREATE INDEX IX_DEMO_BIG_TXN_UPD ON DEMO_BIG_TXN (LAST_UPDATED_TS);

-- ═══════════════════════════════════════════════════════════════════════
-- Row generation
-- ═══════════════════════════════════════════════════════════════════════
-- TOTAL_ROWS         : total rows to create on the Oracle (source) side.
-- BATCH_SIZE         : rows generated + committed per loop iteration
--                      (keeps memory/UNDO flat regardless of TOTAL_ROWS).
--
-- IMPORTANT: keep TOTAL_ROWS in sync with the Snowflake script's
-- TOTAL_ROWS setting, so MISSING_TAIL/MISMATCH_EVERY/DEST_ONLY_ROWS
-- there line up with these Oracle-generated IDs and produce a
-- realistic, predictable mix of HASH_MISMATCH / MISSING_IN_DEST /
-- MISSING_IN_SOURCE diffs.
DECLARE
    TOTAL_ROWS   CONSTANT PLS_INTEGER := 120000;   -- e.g. 100k / 1M / 5M / 20M
    BATCH_SIZE   CONSTANT PLS_INTEGER := 10000;
    v_start      PLS_INTEGER := 1;
    v_end        PLS_INTEGER;
BEGIN
    WHILE v_start <= TOTAL_ROWS LOOP
        v_end := LEAST(v_start + BATCH_SIZE - 1, TOTAL_ROWS);

        INSERT INTO DEMO_BIG_TXN (TXN_ID, CUSTOMER_ID, ACCOUNT_ID, AMOUNT, CURRENCY,
                                   TXN_STATUS, TXN_TYPE, DESCRIPTION, TXN_TS, LAST_UPDATED_TS)
        SELECT
            gen.n,
            1000 + MOD(gen.n, 200000),
            5000 + MOD(gen.n, 50000),
            ROUND(DBMS_RANDOM.VALUE(1, 25000), 2),
            CASE MOD(gen.n, 5)
                WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' WHEN 2 THEN 'GBP'
                WHEN 3 THEN 'BRL' ELSE 'JPY'
            END,
            CASE MOD(gen.n, 5)
                WHEN 0 THEN 'POSTED' WHEN 1 THEN 'PENDING' WHEN 2 THEN 'FAILED'
                WHEN 3 THEN 'REVERSED' ELSE 'SETTLED'
            END,
            CASE MOD(gen.n, 5)
                WHEN 0 THEN 'DEBIT' WHEN 1 THEN 'CREDIT' WHEN 2 THEN 'TRANSFER'
                WHEN 3 THEN 'FEE' ELSE 'REFUND'
            END,
            'Auto-generated txn #' || gen.n,
            TIMESTAMP '2026-01-01 00:00:00' + NUMTODSINTERVAL(MOD(gen.n, 500000), 'SECOND'),
            TIMESTAMP '2026-01-01 00:00:00' + NUMTODSINTERVAL(MOD(gen.n, 500000), 'SECOND')
        FROM (
            SELECT v_start - 1 + LEVEL AS n
            FROM DUAL
            CONNECT BY LEVEL <= (v_end - v_start + 1)
        ) gen;

        COMMIT;
        DBMS_OUTPUT.PUT_LINE('Inserted up to TXN_ID=' || v_end || ' / ' || TOTAL_ROWS);
        v_start := v_end + 1;
    END LOOP;
END;
/

EXEC DBMS_STATS.GATHER_TABLE_STATS(ownname => 'RECON', tabname => 'DEMO_BIG_TXN', cascade => TRUE);

SELECT COUNT(*) AS row_count FROM DEMO_BIG_TXN;

