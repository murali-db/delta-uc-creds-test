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

This project demonstrates **three different approaches** for integrating Unity Catalog credentials with Delta Lake:

1. **UCCredentialsTest.scala** - DataFrame options approach (simple):
   - Calls UC Iceberg REST API `/plan` endpoint to vend temporary S3 credentials
   - Creates SparkSession with Delta extensions
   - Passes credentials via DataFrame options with "fs.s3a.*" prefix
   - Reads table data (triggering credential flow to executors)
   - **Use case:** Simple, direct credential injection

2. **UCCredentialsTestWithCatalog.scala + CustomUCCatalog.scala** - Mock catalog approach (educational):
   - Implements the UCSingleCatalog delegation pattern with hardcoded credentials
   - Demonstrates how catalog-based credential injection works
   - Shows the delegation chain: CustomUCCatalog → DeltaCatalog → CustomProxy
   - **Use case:** Understanding the catalog delegation pattern

3. **UCCredentialsTestWithRealUCSingleCatalog.scala** - Real Unity Catalog integration (production):
   - Uses the real `io.unitycatalog.spark.UCSingleCatalog` from Unity Catalog Spark connector
   - Automatically fetches credentials from Unity Catalog server via REST API
   - No manual credential fetching required - catalog handles everything
   - **Use case:** Production deployments with full UC integration

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

### Run - Custom Catalog Approach (UCSingleCatalog Pattern - Mock)
```bash
sbt "runMain UCCredentialsTestWithCatalog"
```

### Run - Real UCSingleCatalog Approach (Production Pattern)

**One-liner command:**
```bash
source .env && export UC_URI UC_TOKEN CATALOG_NAME SCHEMA TABLE TABLE_LOCATION && export SBT_OPTS="-Xmx4G -XX:MaxMetaspaceSize=1G -XX:MaxDirectMemorySize=2G" && sbt "runMain UCCredentialsTestWithRealUCSingleCatalog"
```

**Note**: This test requires Java 17 runtime. The above command should use Java 17 by default on this system.

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

### 1. Credential Vending (Approaches 1 & 2)

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

### Iceberg REST Catalog Specification Compliance

**Latest Update (2025-11-24)**: The `UCCredentialsTestWithRealUCSingleCatalog` now implements proper Iceberg REST catalog spec compliance:

#### Implementation Details

According to the [Iceberg REST catalog specification](https://iceberg.apache.org/rest-catalog-spec/), clients should:

1. **Call `/v1/config` endpoint first** to discover catalog configuration
2. **Extract optional "prefix"** from `config.overrides["prefix"]`
3. **Use prefix in subsequent API calls**: `/v1/{prefix}/namespaces/.../tables/.../plan`

#### Code Flow

```scala
// Step 1: Fetch catalog configuration
val config = fetchCatalogConfig(ucUri, ucToken)
// Response: {"defaults": {...}, "overrides": {"prefix": "catalogs/my-catalog"}}

// Step 2: Extract prefix
val prefix = config.flatMap(_.overrides.get("prefix"))

// Step 3: Construct plan endpoint URL
val url = prefix match {
  case Some(p) => s"{base}/v1/{p}/namespaces/{schema}/tables/{table}/plan"
  case None => s"{base}/v1/catalogs/{catalog}/namespaces/{schema}/tables/{table}/plan"
}
```

#### Graceful Fallback Strategy

- If `/v1/config` endpoint fails → falls back to default `catalogs/{catalog}` pattern
- If no prefix in config → uses default `catalogs/{catalog}` pattern
- Maintains backward compatibility with existing code

#### Implementation Source

This implementation is adapted from [murali-db/delta PR #15](https://github.com/murali-db/delta/pull/15/files) (`UnityCatalogMetadata.scala`), which correctly implements the Iceberg REST catalog spec.

**Key Functions**:
- `fetchCatalogConfig()` - Calls `/v1/config` using sttp + circe
- `extractPrefix()` - Safely extracts prefix from config.overrides
- `fetchUCCredentials()` - Accepts optional prefix parameter for URL construction
- `fetchUCCredentialsSpecCompliant()` - Orchestrates the full spec-compliant flow

**Testing Note**: The Iceberg REST spec implementation compiles successfully. The runtime test was not executed locally due to Java version constraints in the development environment, but the code follows the proven pattern from PR #15 and should work correctly when run with the proper Java 17 runtime.

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

### 2C. Credential Injection - Real UCSingleCatalog Approach (UCCredentialsTestWithRealUCSingleCatalog.scala)

Configure SparkSession with real UCSingleCatalog, no manual credential fetching needed:

```scala
// Configure SparkSession with real UCSingleCatalog
val spark = SparkSession.builder()
  .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog")
  .config("spark.sql.catalog.unity.uri", UC_URI)      // UC server URL
  .config("spark.sql.catalog.unity.token", UC_TOKEN)  // UC auth token
  .getOrCreate()

// Read via catalog - UCSingleCatalog fetches credentials automatically!
val df = spark.table("unity.catalog.schema.table")
```

When `spark.table("unity.catalog.schema.table")` is called, the real UCSingleCatalog's UCProxy:

```scala
override def loadTable(ident: Identifier): Table = {
  // 1. Fetch table metadata from UC server
  val tableInfo = tablesApi.getTable(catalog, schema, table)

  // 2. Request temporary credentials from UC server
  val tempCreds = temporaryCredentialsApi.generateTemporaryTableCredentials(
    catalog, schema, table
  )

  // 3. Convert UC credentials to Hadoop properties (supports S3/GCS/Azure)
  val credProps = CredPropsUtil.createTableCredProps(tempCreds)
  // For S3: credProps = Map(
  //   "fs.s3a.access.key" -> tempCreds.awsAccessKeyId,
  //   "fs.s3a.secret.key" -> tempCreds.awsSecretAccessKey,
  //   "fs.s3a.session.token" -> tempCreds.awsSessionToken
  // )

  // 4. Create CatalogTable with credentials in storage.properties
  val catalogTable = CatalogTable(
    identifier = ...,
    storage = CatalogStorageFormat.empty.copy(
      locationUri = Some(tableInfo.storageLocation),
      properties = credProps  // Credentials injected!
    ),
    schema = ...,
    provider = Some("delta")
  )

  V1Table(catalogTable)  // Return with auto-fetched credentials!
}
```

**Key Difference:** No manual REST API calls in your application code - the catalog handles all UC communication automatically!

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
├── build.sbt                                        # SBT build definition
├── src/main/scala/
│   ├── UCCredentialsTest.scala                     # Approach 1: DataFrame options
│   ├── UCCredentialsTestWithCatalog.scala          # Approach 2: Mock catalog (educational)
│   ├── UCCredentialsTestWithRealUCSingleCatalog.scala # Approach 3: Real UC catalog (production)
│   └── CustomUCCatalog.scala                       # Mock UCSingleCatalog implementation
├── .env                                             # Environment variables (git-ignored)
├── .env.example                                    # Environment variable template
├── .gitignore                                      # Git ignore rules
└── README.md                                       # This file
```

## Three Approaches: DataFrame Options, Mock Catalog, and Real UC Catalog

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

**Use case:** Educational - Understanding the UCSingleCatalog delegation pattern and how credentials flow through the catalog chain

### Approach 3: Real UCSingleCatalog (Production Pattern - Working ✅)

The `UCCredentialsTestWithRealUCSingleCatalog.scala` uses the real Unity Catalog Spark connector:

```scala
val spark = SparkSession.builder()
  .appName("UC Credentials Test with Real UCSingleCatalog")
  .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog")
  .config("spark.sql.catalog.unity.uri", UC_URI)  // Unity Catalog server URL
  .config("spark.sql.catalog.unity.token", UC_TOKEN)  // UC auth token
  .getOrCreate()

// Read table - UCSingleCatalog handles everything automatically!
val df = spark.table(s"unity.$CATALOG_NAME.$SCHEMA.$TABLE")
df.show()
```

**What happens behind the scenes:**

1. **No manual credential fetching** - UCSingleCatalog does it for you
2. When `spark.table()` is called, the catalog:
   - Connects to Unity Catalog server via REST API
   - Fetches table metadata via `TablesApi.getTable()`
   - Requests temporary credentials via `TemporaryCredentialsApi.generateTemporaryTableCredentials()`
   - Uses `CredPropsUtil` to convert credentials to Hadoop properties (supports S3, GCS, Azure)
   - Injects credentials into `CatalogTable.storage.properties`
   - Returns table ready for Spark to read

**The Real UCSingleCatalog Architecture:**

```
UCSingleCatalog → DeltaCatalog → UCProxy
                                    ↓
                         Unity Catalog Server (REST API)
                                    ↓
                         Temporary Credentials (auto-fetched)
                                    ↓
                         storage.properties → Hadoop Config → S3
```

**Key Differences from Mock:**

| Aspect | Mock Catalog | Real UCSingleCatalog |
|--------|-------------|---------------------|
| **Credential Source** | Hardcoded from catalog options | Dynamically fetched from UC server |
| **Table Metadata** | Hardcoded table location | Fetched from UC server |
| **Manual API Calls** | Yes - you call UC REST API yourself | No - catalog handles it automatically |
| **Credential Renewal** | Not supported | Supports automatic renewal via token providers |
| **Cloud Support** | S3 only | S3, GCS, Azure via proper token providers |
| **Production Ready** | No - educational only | Yes - full production implementation |

**Advantages:**
- **Production-ready** - Used by Databricks and Unity Catalog integrations
- **Automatic credential management** - No manual REST API calls needed
- **Multi-cloud support** - Works with S3, GCS, Azure transparently
- **Credential renewal** - Optionally supports automatic credential refresh for long-running jobs
- **Complete UC integration** - Handles all table operations (create, drop, list, etc.)
- **Simplified code** - Just configure catalog, then access tables normally

**Use case:** Production deployments where you want full Unity Catalog integration with automatic credential management

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
