# Big-table load generators (scale testing)

These two scripts generate a large `DEMO_BIG_TXN` table on both sides so you
can test the reconciler engine's performance and correctness at scale,
instead of only the small hand-written demo datasets.

- `oracle_big_table_setup.sql` — creates `RECON.DEMO_BIG_TXN` and bulk-inserts
  `TOTAL_ROWS` rows in `BATCH_SIZE` commits (default 120,000 rows / 10,000
  per batch). Batching keeps UNDO usage flat no matter how big `TOTAL_ROWS` is.
- `snowflake_big_table_setup.sql` — creates the matching `RECON_DB.PUBLIC.DEMO_BIG_TXN`
  table using `TABLE(GENERATOR(ROWCOUNT => ...))`, Snowflake's fastest
  bulk-generation method, and deliberately introduces:
  - **`MISSING_TAIL`** Oracle-only rows at the end of the ID range → `MISSING_IN_DEST`
  - **`MISMATCH_EVERY`** Nth row with a changed `AMOUNT` → `HASH_MISMATCH`
  - **`DEST_ONLY_ROWS`** extra Snowflake-only rows → `MISSING_IN_SOURCE`

## How to run

1. Edit the tunables at the top of each script (they must agree with each
   other — `TOTAL_ROWS` in particular):
   ```sql
   -- oracle_big_table_setup.sql
   TOTAL_ROWS CONSTANT PLS_INTEGER := 120000;

   -- snowflake_big_table_setup.sql
   SET TOTAL_ROWS = 120000;
   ```
   Start small (e.g. `10000`) the first time to confirm everything wires up
   correctly end-to-end, then scale up to millions of rows.

2. Run against Oracle:
   ```bash
   sqlplus RECON_APP/<pwd>@//localhost:1521/FREEPDB1 @db/demo/oracle_big_table_setup.sql
   ```

3. Run against Snowflake:
   ```bash
   snowsql -a <account> -u <user> -f db/demo/snowflake_big_table_setup.sql
   ```

4. Point the app at the new table. In the UI (`Configure Table`), set:
   - Source table: `RECON.DEMO_BIG_TXN` (or whatever your Oracle schema is)
   - Dest table: `RECON_DB.PUBLIC.DEMO_BIG_TXN`
   - Primary key: `TXN_ID`
   - Hash columns: everything except `TXN_ID` (or use "Select all")
   - Modified column: `LAST_UPDATED_TS` (enables incremental/buffered comparison)

5. Run the job (or a batch run) and watch the Diff Log / summary bar. With the
   defaults above you should see roughly:
   - `HASH_MISMATCH`: `TOTAL_ROWS / MISMATCH_EVERY` (~876 for 120k/137)
   - `MISSING_IN_DEST`: `MISSING_TAIL` (1,200)
   - `MISSING_IN_SOURCE`: `DEST_ONLY_ROWS` (500)

## Scaling tips

- Bump `TOTAL_ROWS` incrementally (100k → 1M → 5M → 20M+) to find the point
  where batch size, JDBC fetch size, or JVM heap need tuning
  (`OracleBatchIterator`, `SnowflakeRowIterator`, `application.yml`).
- Increase Oracle `BATCH_SIZE` (e.g. 100k–200k) if generation itself is the
  bottleneck and your UNDO tablespace can handle it.
- Re-run the Snowflake script alone (with different `MISMATCH_EVERY` /
  `DEST_ONLY_ROWS` values) without regenerating Oracle data, to test how the
  engine behaves as the *proportion* of diffs changes without changing volume.
- Use the app's `bufferMinutes` + `modifiedColumn` (incremental compare) to
  test that only rows near the boundary get rescanned on subsequent runs,
  instead of a full table scan every time.

