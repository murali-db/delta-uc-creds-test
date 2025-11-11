# Delta UC Credentials Test

A demonstration application showing how Unity Catalog credentials flow through Delta Lake's catalog system to enable S3 file access.

## Overview

This application demonstrates the complete credential flow from Unity Catalog through Delta Lake:

```
UC Iceberg REST API
  ↓ (vend credentials)
CustomDeltaCatalog.loadTable()
  ↓ (inject into storage.properties)
CatalogTable.storage.properties
  ↓ (filter "fs.*" keys)
DeltaLog.options
  ↓ (create Hadoop Configuration)
spark.sessionState.newHadoopConfWithOptions()
  ↓ (set on Configuration)
Hadoop Configuration (driver + executors)
  ↓ (S3AFileSystem reads config)
Executors read S3 files successfully!
```

## Architecture

### Components

1. **UCCredentialsTest.scala** - Main application that:
   - Calls UC Iceberg REST API `/plan` endpoint to vend temporary S3 credentials
   - Configures SparkSession with CustomDeltaCatalog
   - Registers a Delta table with S3 location
   - Reads table data (triggering credential flow)

2. **CustomDeltaCatalog.scala** - Custom catalog that:
   - Extends DeltaCatalog
   - Intercepts `loadTable()` calls
   - Injects UC credentials into `CatalogTable.storage.properties` with "fs.*" prefix
   - Returns updated DeltaTableV2

### Key Mechanism

The "fs.*" prefix is critical! Delta Lake filters `CatalogTable.storage.properties` and only passes through keys starting with "fs." or "dfs." to the Hadoop Configuration. This is why we transform:

```scala
// UC Iceberg REST response:
"s3.access-key-id" → "fs.s3a.access.key"
"s3.secret-access-key" → "fs.s3a.secret.key"
"s3.session-token" → "fs.s3a.session.token"
```

## Prerequisites

### Local Maven Dependencies

This project requires locally published versions of:
- **Apache Spark 4.0.2-SNAPSHOT**
- **Delta Lake 4.0.0**
- **Unity Catalog Spark Connector 0.3.0-SNAPSHOT**

Build and publish these dependencies to your local Maven repository (`~/.m2/repository`) before running this application.

### Unity Catalog Access

You need access to a Unity Catalog instance with:
- Databricks workspace URL
- Personal Access Token (PAT)
- A Delta/Iceberg table with S3 storage

## Setup

1. **Clone the repository**
   ```bash
   git clone <your-repo-url>
   cd delta-uc-creds-test
   ```

2. **Configure environment variables**

   Copy the example environment file:
   ```bash
   cp .env.example .env
   ```

   Edit `.env` with your credentials:
   ```bash
   UC_URI=https://your-workspace.cloud.databricks.com
   UC_TOKEN=dapi... # Your Databricks PAT
   CATALOG_NAME=your_catalog
   SCHEMA=your_schema
   TABLE=your_table
   TABLE_LOCATION=s3://your-bucket/path/to/table
   ```

3. **Load environment variables**
   ```bash
   source .env
   export UC_URI UC_TOKEN CATALOG_NAME SCHEMA TABLE TABLE_LOCATION
   ```

## Running the Application

### Compile
```bash
sbt compile
```

### Run
```bash
sbt "runMain UCCredentialsTest"
```

### Expected Output

```
================================================================================
UC Credentials Test Application
================================================================================

[Step 1] Calling UC Iceberg REST API to vend credentials...
  ✓ Received credentials
    Access Key ID: ASIA...
    Expires At: Mon Nov 11 14:29:24 UTC 2025
    Region: us-west-2

[Step 2] Creating SparkSession with CustomDeltaCatalog...
  ✓ SparkSession created with CustomDeltaCatalog

[Step 3] Creating CatalogTable with S3 location...
  Table Location: s3://your-bucket/path/to/table
  ✓ Table registered: uc.your_schema.your_table

[Step 4] Loading table via CustomDeltaCatalog...
[CustomDeltaCatalog] Injecting UC credentials for table: your_schema.your_table
[CustomDeltaCatalog] Access Key ID: ASIA...
[CustomDeltaCatalog] Expires At: 1762871364000
  ✓ Table loaded successfully

[Step 5] Reading table data (using UC-vended credentials)...

Table contents:
+----+------+
| id | name |
+----+------+
| 1  | test |
+----+------+

  ✓ Successfully read 4 rows using UC-vended credentials!

================================================================================
SUCCESS: Credentials flowed correctly!
  CatalogTable.storage.properties → DeltaLog.options → Hadoop Configuration
================================================================================
```

## How It Works

### 1. Credential Vending (UCCredentialsTest.scala)

The application calls UC's Iceberg REST API:

```scala
POST /api/2.1/unity-catalog/iceberg-rest/v1/catalogs/{catalog}/namespaces/{schema}/tables/{table}/plan
Authorization: Bearer {UC_TOKEN}
Body: {"snapshot-id": -1}
```

Response includes:
```json
{
  "storage-credentials": [{
    "config": {
      "s3.access-key-id": "ASIA...",
      "s3.secret-access-key": "...",
      "s3.session-token": "...",
      "s3.session-token-expires-at-ms": "1762871364000"
    }
  }]
}
```

### 2. Credential Injection (CustomDeltaCatalog.scala)

When `spark.table("uc.schema.table")` is called:

```scala
override def loadTable(ident: Identifier): Table = {
  val table = super.loadTable(ident).asInstanceOf[DeltaTableV2]

  // Create storage properties with "fs.*" prefix
  val ucStorageProps = Map(
    "fs.s3a.access.key" -> creds.accessKeyId,
    "fs.s3a.secret.key" -> creds.secretAccessKey,
    "fs.s3a.session.token" -> creds.sessionToken
  )

  // Inject into catalogTable.storage.properties
  val updatedCatalogTable = table.catalogTable.map { ct =>
    ct.copy(storage = ct.storage.copy(
      properties = ct.storage.properties ++ ucStorageProps
    ))
  }

  DeltaTableV2(..., catalogTable = updatedCatalogTable, ...)
}
```

### 3. Flow to Hadoop Configuration

Delta Lake automatically:
1. Extracts properties starting with "fs.*" from `storage.properties`
2. Passes them to `DeltaLog.forTable(..., options = fileSystemOptions)`
3. Stores in `DeltaLog.options` field
4. Creates Hadoop Configuration via `spark.sessionState.newHadoopConfWithOptions(options)`
5. S3AFileSystem reads credentials from Configuration on executors

### 4. File Access

When `.show()` or `.count()` is called:
```scala
table.show()
  → DeltaTableV2.toDf
  → toBaseRelation
  → deltaLog.createRelation()
  → HadoopFsRelation created with options (containing credentials)
  → Executors use S3AFileSystem with credentials from Hadoop Configuration
  → Successfully read S3 files!
```

## Code Structure

```
delta-uc-creds-test/
├── build.sbt                              # SBT build definition
├── src/main/scala/
│   ├── UCCredentialsTest.scala           # Main application
│   └── CustomDeltaCatalog.scala          # Custom catalog with credential injection
├── .env.example                          # Environment variable template
├── .gitignore                            # Git ignore rules
└── README.md                             # This file
```

## Key Insights

1. **"fs." Prefix Requirement**: Delta filters `CatalogTable.storage.properties` and only passes keys starting with "fs." or "dfs." to Hadoop Configuration.

2. **Storage Properties Flow**: Properties flow: `storage.properties` → `DeltaLog.options` → `newHadoopConfWithOptions()` → `Configuration.set(key, value)`

3. **Executor-Side Access**: Hadoop Configuration is serialized and sent to executors, enabling them to access S3 directly with vended credentials.

4. **V1Table Wrapper**: Delta's catalog returns `DeltaTableV2`, which wraps `CatalogTable` containing our injected credentials.

## Troubleshooting

### Credentials not found
- Ensure environment variables are exported: `export UC_URI=...`
- Check `.env` file has correct values
- Verify you sourced the file: `source .env`

### S3 Access Denied
- Verify UC token has permissions to the table
- Check table location is correct
- Ensure credentials haven't expired (typically 1 hour)

### Compilation Errors
- Verify Spark 4.0.2-SNAPSHOT is in `~/.m2/repository`
- Verify Delta 4.0.0 is published to local Maven
- Run `sbt clean compile`

## References

- [Delta Lake Documentation](https://docs.delta.io/)
- [Unity Catalog Documentation](https://docs.databricks.com/unity-catalog/)
- [Apache Spark Catalog API](https://spark.apache.org/docs/latest/sql-data-sources-v2.html)

## License

This is a demonstration/test application. Adjust as needed for your use case.
