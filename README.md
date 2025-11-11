# Delta UC Credentials Test

A demonstration application showing how Unity Catalog credentials flow through Delta Lake to enable S3 file access.

## Overview

This application demonstrates the complete credential flow from Unity Catalog through Delta Lake:

```
UC Iceberg REST API
  ↓ (vend credentials)
DataFrame.read.options (with "fs.s3a.*" keys)
  ↓ (Delta internally processes)
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

1. **UCCredentialsTest.scala** - Main working application that:
   - Calls UC Iceberg REST API `/plan` endpoint to vend temporary S3 credentials
   - Creates SparkSession with Delta extensions
   - Passes credentials via DataFrame options with "fs.s3a.*" prefix
   - Reads table data (triggering credential flow to executors)

2. **CustomDeltaCatalog.scala** - Reference implementation showing catalog-based approach:
   - Extends DeltaCatalog to intercept `loadTable()` calls
   - Injects UC credentials into `CatalogTable.storage.properties` or `table.options`
   - **Note**: Currently not used due to delegate catalog setup complexity (see below)

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

### Run - DataFrame Options Approach (Simple)
```bash
sbt "runMain UCCredentialsTest"
```

### Run - Custom Catalog Approach (UCSingleCatalog Pattern)
```bash
sbt "runMain UCCredentialsTestWithCatalog"
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

Both approaches start with the same credential vending step:

### 1. Credential Vending

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

### 2A. Credential Injection - DataFrame Options Approach (UCCredentialsTest.scala)

Pass credentials directly via DataFrame options:

```scala
val credentialOptions = Map(
  "fs.s3a.access.key" -> credentials.accessKeyId,
  "fs.s3a.secret.key" -> credentials.secretAccessKey,
  "fs.s3a.session.token" -> credentials.sessionToken
)

val df = spark.read
  .format("delta")
  .options(credentialOptions)
  .load(s3aLocation)
```

### 2B. Credential Injection - Catalog Approach (CustomUCCatalog.scala)

Configure catalog with credentials, then read via catalog:

```scala
// Configure SparkSession with custom catalog
val spark = SparkSession.builder()
  .config("spark.sql.catalog.uc", "CustomUCCatalog")
  .config("spark.sql.catalog.uc.credentials", credentials.toJson)
  .config("spark.sql.catalog.uc.table_location", s3aLocation)
  .getOrCreate()

// Read via catalog (triggers credential injection)
val df = spark.table("uc.schema.table")
```

When `spark.table("uc.schema.table")` is called, CustomProxy injects credentials:

```scala
override def loadTable(ident: Identifier): Table = {
  val creds = parseCredentials(options.get("credentials"))
  val tablePath = options.get("table_location")

  // Create CatalogTable with credentials in storage.properties
  val catalogTable = CatalogTable(
    identifier = TableIdentifier(ident.name(), ident.namespace().headOption),
    tableType = CatalogTableType.EXTERNAL,
    storage = CatalogStorageFormat.empty.copy(
      locationUri = Some(CatalogUtils.stringToURI(tablePath)),
      properties = Map(
        "fs.s3a.access.key" -> creds.accessKeyId,
        "fs.s3a.secret.key" -> creds.secretAccessKey,
        "fs.s3a.session.token" -> creds.sessionToken
      )
    ),
    schema = StructType(Seq.empty),
    provider = Some("delta")
  )

  V1Table(catalogTable)  // Return with credentials!
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
│   ├── UCCredentialsTest.scala           # Simple DataFrame options approach
│   ├── UCCredentialsTestWithCatalog.scala # Custom catalog approach test
│   └── CustomUCCatalog.scala             # UCSingleCatalog pattern implementation
├── .env                                   # Environment variables (git-ignored)
├── .env.example                          # Environment variable template
├── .gitignore                            # Git ignore rules
└── README.md                             # This file
```

## Two Approaches: DataFrame Options vs Custom Catalog

### Approach 1: DataFrame Options (Simple - Working ✅)

The current `UCCredentialsTest.scala` uses the simple and direct approach:

```scala
val credentialOptions = Map(
  "fs.s3a.access.key" -> credentials.accessKeyId,
  "fs.s3a.secret.key" -> credentials.secretAccessKey,
  "fs.s3a.session.token" -> credentials.sessionToken
)

val table = spark.read
  .format("delta")
  .options(credentialOptions)
  .load(s3aLocation)
```

**Advantages:**
- Simple, straightforward
- No custom catalog setup required
- Works immediately with `spark.databricks.delta.loadFileSystemConfigsFromDataFrameOptions=true`
- Credentials flow: DataFrame options → DeltaLog.options → Hadoop Configuration → S3

**Use case:** Direct table access where you control the SparkSession and DataFrame read options

### Approach 2: Custom Catalog (UCSingleCatalog Pattern - Working ✅)

The `CustomUCCatalog.scala` implements the UCSingleCatalog pattern from Unity Catalog:

```scala
class CustomUCCatalog extends TableCatalog with SupportsNamespaces {
  @volatile private var delegate: TableCatalog = null

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // Create internal proxy that will inject credentials
    val proxy = new CustomProxy(options)
    proxy.initialize(name, options)

    // Create DeltaCatalog and set its delegate to proxy (KEY!)
    delegate = Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getDeclaredConstructor().newInstance().asInstanceOf[TableCatalog]
    delegate.asInstanceOf[DelegatingCatalogExtension].setDelegateCatalog(proxy)
  }

  override def loadTable(ident: Identifier): Table = delegate.loadTable(ident)
}

private class CustomProxy(options: CaseInsensitiveStringMap)
    extends TableCatalog with SupportsNamespaces {
  override def loadTable(ident: Identifier): Table = {
    // Parse credentials from catalog options
    val creds = parseCredentials(options.get("credentials"))
    val tablePath = options.get("table_location")

    // Create CatalogTable with credentials in storage.properties
    val catalogTable = CatalogTable(
      identifier = TableIdentifier(ident.name(), ident.namespace().headOption),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(
        locationUri = Some(CatalogUtils.stringToURI(tablePath)),
        properties = Map(
          "fs.s3a.access.key" -> creds.accessKeyId,
          "fs.s3a.secret.key" -> creds.secretAccessKey,
          "fs.s3a.session.token" -> creds.sessionToken
        )
      ),
      schema = StructType(Seq.empty),
      provider = Some("delta")
    )

    // Return V1Table with credentials
    V1Table(catalogTable)
  }
}
```

**The Key Pattern - UCSingleCatalog:**

Instead of extending DeltaCatalog (which causes delegate chain problems), the pattern is:
1. Implement `TableCatalog` directly (don't extend DeltaCatalog)
2. Create DeltaCatalog as a delegate field
3. Set DeltaCatalog's delegate to your internal proxy
4. Proxy injects credentials into CatalogTable.storage.properties

**Flow:** `CustomUCCatalog → DeltaCatalog → CustomProxy → Credentials Injected`

**Critical Requirement - Use Catalog-Based Reads:**

```scala
// ✅ THIS WORKS - Uses catalog
val df = spark.table("uc.schema.table")

// ❌ THIS FAILS - Bypasses catalog
val df = spark.read.format("delta").load(path)
```

Path-based reads create a fresh DeltaLog that bypasses catalog-injected credentials!

**Advantages:**
- Centralized credential management
- Follows Unity Catalog's proven pattern
- Credentials automatically injected for all catalog-based table accesses
- Clean delegation chain without recursion issues

**Use case:** Multi-user environments where credentials should be transparently injected for catalog-based table access

## Key Insights

1. **"fs." Prefix Requirement**: Delta filters options and only passes keys starting with "fs." or "dfs." to Hadoop Configuration.

2. **DataFrame Options Flow**: Options flow: `DataFrame.read.options()` → `DeltaLog.options` → `newHadoopConfWithOptions()` → `Configuration.set(key, value)`

3. **Executor-Side Access**: Hadoop Configuration is serialized and sent to executors, enabling them to access S3 directly with vended credentials.

4. **Credential Refresh**: For longer-running jobs, implement `AwsCredentialsProvider` that calls UC REST API to refresh credentials before expiry.

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
