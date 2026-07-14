# ⚡ Cloud Data Reconciliation Engine

A high-performance **Sort-Merge Zipper** tool that compares massive tables between an **Oracle** production database and a **Snowflake** data warehouse, designed for CDC-driven environments (Debezium, Kafka Connect, etc.).

## Contribution Requirements

All contributions and code changes must comply with `instruction.md`.
By submitting changes, you confirm alignment with the standards in `instruction.md`.

---

## Architecture

```
┌──────────────┐   JDBC Streaming    ┌───────────────────────┐   JDBC Streaming    ┌──────────────┐
│  Oracle DB   │ ─────────────────►  │  Sort-Merge Zipper    │ ◄───────────────── │  Snowflake   │
│  (Source)    │   ORDER BY PK       │  CompareEngine.java   │   ORDER BY PK      │  (Dest)      │
└──────────────┘   + MD5 hash        │                       │   + MD5 hash       └──────────────┘
                                     │  Diff → SQLite        │
                                     └───────────┬───────────┘
                                                 │
                                     ┌───────────▼───────────┐
                                     │  Javalin REST API     │
                                     │  + Dark-mode UI       │
                                     │  http://localhost:7070 │
                                     └───────────────────────┘
```

### Key Design Patterns
| Pattern | Where |
|---|---|
| **Strategy** | `QueryStrategy` → `OracleQueryStrategy` / `SnowflakeQueryStrategy` |
| **Repository** | `DiffRepository` (SQLite abstraction) |
| **Sort-Merge Join** | `CompareEngine` (streaming zipper loop) |
| **Buffer Window** | 15-min high-water mark filter to eliminate CDC false positives |

---

## Prerequisites

- **Java 17+**
- **Maven 3.8+**
- Network access to your Oracle and Snowflake instances
- Docker (optional, for containerised deployment)

---

## Environment Variables

| Variable | Description | Example |
|---|---|---|
| `ORACLE_JDBC_URL` | Oracle JDBC connection string | `jdbc:oracle:thin:@//host:1521/ORCL` |
| `ORACLE_USER` | Oracle username | `recon_reader` |
| `ORACLE_PASSWORD` | Oracle password | `••••••` |
| `SNOWFLAKE_JDBC_URL` | Snowflake JDBC connection string | `jdbc:snowflake://acct.snowflakecomputing.com` |
| `SNOWFLAKE_USER` | Snowflake username | `recon_svc` |
| `SNOWFLAKE_PASSWORD` | Snowflake password | `••••••` |
| `SQLITE_PATH` | Path to local diff DB file (optional) | `reconciler_diffs.db` |

Create a `.env` file for local development (never commit this):

```bash
export ORACLE_JDBC_URL="jdbc:oracle:thin:@//localhost:1521/XEPDB1"
export ORACLE_USER="system"
export ORACLE_PASSWORD="oracle"
export SNOWFLAKE_JDBC_URL="jdbc:snowflake://xy12345.us-east-1.snowflakecomputing.com/?db=DWH&warehouse=COMPUTE_WH"
export SNOWFLAKE_USER="recon_user"
export SNOWFLAKE_PASSWORD="s3cur3"
```

## Spring-Style YAML Config (No Spring Boot)

You can avoid exporting variables on every run by creating `application.yml` in the project root.

```yaml
oracle:
  jdbcUrl: "jdbc:oracle:thin:@//localhost:1521/FREEPDB1"
  user: "RECON"
  password: "recon"

snowflake:
  jdbcUrl: "jdbc:snowflake://<account>.snowflakecomputing.com/?warehouse=RECON_WH&db=RECON_DB&schema=PUBLIC&role=ACCOUNTADMIN"
  user: "<user>"
  password: "<password>"

sqlite:
  path: "reconciler_diffs.db"
```

Resolution order is:
1. Environment variable
2. `application.yml`
3. Hard-coded default (only for SQLite path)

### Scalable Multi-Schema Mapping

To reconcile many Oracle schemas into one Snowflake account/database, define schema mappings once and then run batch jobs by `schemaId`.

```yaml
reconciler:
  mappings:
    - schema-id: TENANT_US
      oracle-schema: SRC_US
      snowflake-database: LANDING_DB
      snowflake-schema: PUBLIC
      dest-filter-column: SOURCE_SCHEMA_ID
      dest-filter-value: TENANT_US
      enabled: true
    - schema-id: TENANT_EU
      oracle-schema: SRC_EU
      snowflake-database: LANDING_DB
      snowflake-schema: PUBLIC
      dest-filter-column: SOURCE_SCHEMA_ID
      dest-filter-value: TENANT_EU
      enabled: true
```

Use `dest-filter-column` + `dest-filter-value` when multiple Oracle schemas land into shared Snowflake tables.
If both fields are blank, no destination row filter is applied.

Use `POST /api/start-batch` with `schemaIds`:

```json
{
  "schemaIds": ["TENANT_US", "TENANT_EU"],
  "tables": ["ORDERS", "ORDER_LINES"],
  "modifiedColumn": "WHEN_MODIFIED",
  "bufferMinutes": 15,
  "clearBefore": false
}
```

If `schemaIds` is omitted, all enabled mappings are selected.

---

## Build & Run

### 1. Compile the Fat JAR

```bash
mvn clean package -DskipTests
```

### 2. Run locally

```bash
source .env
java --add-opens=java.base/java.nio=ALL-UNNAMED -jar target/compare-engine-1.0.0.jar
```

Open **http://localhost:7070** in your browser.

### 3. Docker

```dockerfile
# Dockerfile
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY target/compare-engine-1.0.0.jar app.jar
EXPOSE 7070
ENTRYPOINT ["java", "-jar", "app.jar"]
```

```bash
docker build -t reconciler .
docker run -p 7070:7070 \
  -e ORACLE_JDBC_URL="jdbc:oracle:thin:@//host:1521/ORCL" \
  -e ORACLE_USER="user" \
  -e ORACLE_PASSWORD="pass" \
  -e SNOWFLAKE_JDBC_URL="jdbc:snowflake://acct.snowflakecomputing.com/?db=DWH" \
  -e SNOWFLAKE_USER="user" \
  -e SNOWFLAKE_PASSWORD="pass" \
  reconciler
```

---

## REST API

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/start-job` | Start a reconciliation job (JSON body with table config) |
| `GET` | `/api/results?page=1&size=100&search=` | Fetch paginated diff results |
| `DELETE` | `/api/results` | Clear all stored diffs |

### POST body example

```json
{
  "sourceTable": "PROD.ORDERS",
  "destTable": "DWH.PUBLIC.ORDERS",
  "primaryKeys": "order_id~line_id",
  "hashColumns": ["amount", "status", "ship_date"],
  "modifiedColumn": "when_modified",
  "bufferMinutes": 15
}
```

---

## Frontend Structure

The UI is now split into a simple static structure so it can grow without keeping everything inside one file:

- `src/main/resources/public/index.html` - Bootstrap-based app shell and layout
- `src/main/resources/public/assets/css/app.css` - shared visual theme and component styles
- `src/main/resources/public/assets/js/app.js` - UI behavior, API calls, polling, AG Grid, modal logic

This keeps the existing endpoint contract and IDs stable while making it easier to evolve toward a fuller framework setup later.

---

## Demo Dataset (5 Extra Tables)

Added demo scripts create five additional table pairs with intentionally different data for reconciliation testing:

- `DEMO_CUSTOMERS`
- `DEMO_PRODUCTS`
- `DEMO_ORDERS`
- `DEMO_PAYMENTS`
- `DEMO_INVENTORY`

Scripts:

- `db/demo/oracle_demo_setup.sql`
- `db/demo/oracle_multi_schema_setup.sql` (creates `ORACLE_SCHEMA1` and `ORACLE_SCHEMA2` with variant data)
- `db/demo/oracle_schema2_migration.sql` (optional migration from legacy `ORACLE_SCHEM2` to `ORACLE_SCHEMA2`)
- `db/demo/snowflake_demo_setup.sql`
- `db/demo/oracle_schema3_setup.sql` (creates `ORACLE_SCHEMA3` with 3 different table names)
- `db/demo/snowflake_schema3_setup.sql` (Snowflake companion for the `SC3_*` tables)

Expected outcomes include a mix of:

- hash mismatches
- rows missing in destination
- rows missing in source

Run scripts in each database client (adjust schema/database names if needed):

```sql
-- Oracle
@db/demo/oracle_demo_setup.sql

-- Snowflake
!source db/demo/snowflake_demo_setup.sql
```

Then in UI batch mode, scan and run tables with `DEMO_` prefix.

---

## How It Works

1. **Query Generation** — The Strategy pattern builds dialect-specific SQL that pushes MD5 hashing and composite-key concatenation to each database engine.
2. **Buffer Window** — A `WHERE modified_col < NOW() - 15 min` clause filters out rows that may still be in-flight through the CDC pipeline, eliminating false positives.
3. **Streaming Cursors** — JDBC `setFetchSize(50000)` ensures rows are streamed, never loaded fully into memory.
4. **Sort-Merge Zipper** — Both result sets are sorted by composite PK. The engine walks them in lock-step, detecting mismatches, missing rows in source, and missing rows in destination in O(n) time.
5. **SQLite Logging** — Differences are batch-inserted into a local SQLite database for querying via the REST API and UI.

---

## License

MIT — use freely, no warranty.
