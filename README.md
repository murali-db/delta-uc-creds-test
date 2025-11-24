# Delta UC Credentials Test

Demonstration of Unity Catalog credential flow through Delta Lake for S3 access.

## Quick Start

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env with your UC credentials

# 2. Run test (Java 17 configured automatically)
source .env && \
  export UC_URI UC_TOKEN CATALOG_NAME SCHEMA TABLE TABLE_LOCATION && \
  export SBT_OPTS="-Xmx4G -XX:MaxMetaspaceSize=1G -XX:MaxDirectMemorySize=2G" && \
  sbt "runMain UCCredentialsTestWithRealUCSingleCatalog"
```

## Three Approaches

1. **UCCredentialsTest.scala** - DataFrame options (simple)
   - Manually fetch credentials from UC `/plan` endpoint
   - Pass credentials via DataFrame.read.options()

2. **UCCredentialsTestWithCatalog.scala** - Mock catalog (educational)
   - Custom catalog implementing UCSingleCatalog delegation pattern

3. **UCCredentialsTestWithRealUCSingleCatalog.scala** - Production pattern
   - Real `io.unitycatalog.spark.UCSingleCatalog` integration
   - Automatic credential fetching
   - **Implements Iceberg REST catalog spec** (config endpoint + prefix discovery)

## Prerequisites

- **Java 17** (Spark 4.0.2-SNAPSHOT requirement)
- Local Maven dependencies: Spark 4.0.2-SNAPSHOT, Delta 4.0.0, UC Connector 0.3.0-SNAPSHOT
- Unity Catalog access (workspace URL, PAT, table with S3 storage)

**Note**: System default Java changed to 11 on Nov 20. The `.env` file auto-configures Java 17.

## Iceberg REST Catalog Spec Compliance

The `UCCredentialsTestWithRealUCSingleCatalog` implements [Iceberg REST catalog spec](https://iceberg.apache.org/rest-catalog-spec/):

1. **Config Discovery**: Calls `GET /v1/config` to retrieve catalog configuration
2. **Prefix Extraction**: Extracts optional `prefix` from `config.overrides["prefix"]`
3. **Dynamic URLs**: Uses prefix in plan endpoint: `/v1/{prefix}/namespaces/.../tables/.../plan`
4. **Graceful Fallback**: Falls back to `catalogs/{catalog}` if config fails

**Implementation**: Adapted from [murali-db/delta PR #15](https://github.com/murali-db/delta/pull/15/files) (UnityCatalogMetadata.scala)

**Key Functions**:
- `fetchCatalogConfig()` - Calls config endpoint (sttp + circe)
- `extractPrefix()` - Safely extracts prefix from config
- `fetchUCCredentials(prefix: Option[String])` - URL construction with optional prefix
- `fetchUCCredentialsSpecCompliant()` - Orchestrates spec-compliant flow

**Testing Note**: Compiles successfully. Runtime test not executed locally due to Java version constraints in dev environment, but follows proven PR #15 pattern.

## Expected Output

```
UC Credentials Test: Three Approaches

Testing Approach 1: Path-based with manual /plan endpoint credentials
→ Table output: 5 rows. This is expected.

Testing Approach 2: UCSingleCatalog with automatic credential fetching
→ Table output: 5 rows. This is expected.

Testing Approach 3: CredPropsUtil pattern (simulating future non-vending UC)
→ Testing with stripped credentials (simulating non-vending UC)
  Unable to read table: Access Denied. This is expected.
→ Testing with re-injected credentials via CredPropsUtil
  Table output: 5 rows. This is expected.
```

## Key Mechanism

Delta Lake filters `CatalogTable.storage.properties` and only passes keys starting with `fs.*` or `dfs.*` to Hadoop Configuration:

```scala
// UC vends: "s3.access-key-id"
// Transform to: "fs.s3a.access.key"  ← Required prefix!
```

## Credential Flow

```
UC REST API → Fetch credentials
  ↓
DataFrame.read.options("fs.s3a.*")  OR  CatalogTable.storage.properties
  ↓
DeltaLog.options
  ↓
spark.sessionState.newHadoopConfWithOptions()
  ↓
Hadoop Configuration (serialized to executors)
  ↓
S3AFileSystem reads credentials
  ↓
Executors access S3 files ✓
```

## Detailed Documentation

### Architecture

**Approach 1: Path-based**
- Manual credential fetching via `/plan` endpoint
- Direct DataFrame options injection
- Works with any Iceberg REST catalog

**Approach 2: Mock Catalog**
- Demonstrates UCSingleCatalog delegation: `CustomUCCatalog → DeltaCatalog → CustomProxy`
- Educational: shows how catalog-based credential injection works
- Hardcoded credentials for testing

**Approach 3: Real UCSingleCatalog**
- Production-ready Unity Catalog integration
- Automatic credential management (no manual REST calls)
- Multi-cloud support (S3, GCS, Azure)
- Optional credential renewal for long-running jobs
- Spec-compliant config discovery and prefix handling

### Code Structure

```
src/main/scala/
  ├── UCCredentialsTest.scala                     # Approach 1
  ├── UCCredentialsTestWithCatalog.scala          # Approach 2
  ├── UCCredentialsTestWithRealUCSingleCatalog.scala  # Approach 3
  └── CustomUCCatalog.scala                       # Mock catalog
```

### Catalog Delegation Pattern (Approach 2)

```scala
CustomUCCatalog extends TableCatalog {
  @volatile private var delegate: DeltaCatalog = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    val proxy = new CustomProxy(options)  // Inject credentials here
    delegate = new DeltaCatalog()
    delegate.setDelegateCatalog(proxy)    // Key: Set DeltaCatalog's delegate
  }
}

class CustomProxy extends TableCatalog {
  override def loadTable(ident: Identifier): Table = {
    // Create CatalogTable with credentials in storage.properties
    val catalogTable = CatalogTable(
      storage = CatalogStorageFormat(properties = Map(
        "fs.s3a.access.key" -> creds.accessKeyId,
        "fs.s3a.secret.key" -> creds.secretAccessKey,
        "fs.s3a.session.token" -> creds.sessionToken
      ))
    )
    V1Table(catalogTable)
  }
}
```

**Critical**: Use `spark.table("catalog.schema.table")` (not path-based reads) to trigger catalog.

### Real UCSingleCatalog Flow (Approach 3)

1. Configure catalog: `spark.sql.catalog.unity = io.unitycatalog.spark.UCSingleCatalog`
2. Call: `spark.table("unity.catalog.schema.table")`
3. UCSingleCatalog → UCProxy:
   - Calls UC server `/v1/config` (discovers prefix)
   - Calls UC server `/v1/{prefix}/.../tables/...` (gets metadata)
   - Requests temp credentials from UC
   - Uses `CredPropsUtil.createTableCredProps()` to convert credentials
   - Injects into `CatalogTable.storage.properties`
4. Delta reads table with embedded credentials

### CredPropsUtil Pattern (Approach 3 Demo)

Shows how to handle future UC servers that don't vend credentials:

```scala
// 1. Load table from UCSingleCatalog (gets metadata without creds)
val table = catalog.loadTable(ident)

// 2. Strip credentials to simulate non-vending UC
val strippedProps = table.properties.filterNot(_._1.startsWith("fs.s3a"))

// 3. Read fails (proves credentials required)
spark.read.options(strippedProps).load(path)  // ✗ Access Denied

// 4. Fetch credentials from /plan endpoint
val creds = fetchUCCredentials(...)

// 5. Convert to UC TemporaryCredentials
val awsCreds = new AwsCredentials()
  .accessKeyId(creds.accessKeyId)
  .secretAccessKey(creds.secretAccessKey)
  .sessionToken(creds.sessionToken)
val tempCreds = new TemporaryCredentials().awsTempCredentials(awsCreds)

// 6. Use CredPropsUtil (same as UCSingleCatalog uses internally!)
val credProps = CredPropsUtil.createTableCredProps(
  false, "s3", ucUri, ucToken, tableId, TableOperation.READ, tempCreds
).asScala.toMap

// 7. Merge using ++ operator (overwrites existing keys)
val mergedProps = strippedProps ++ credProps

// 8. Read succeeds!
spark.read.options(mergedProps).load(path)  // ✓ SUCCESS
```

## Troubleshooting

**Java Version Error**:
```bash
java -version  # Check version
source .env    # Configures Java 17
```

**Credentials Not Found**:
```bash
source .env
export UC_URI UC_TOKEN CATALOG_NAME SCHEMA TABLE TABLE_LOCATION
```

**S3 Access Denied**:
- Verify UC token has table permissions
- Check credentials haven't expired (typically 1 hour TTL)

**Compilation Errors**:
```bash
sbt clean compile
```

## References

- [Iceberg REST Catalog Spec](https://iceberg.apache.org/rest-catalog-spec/)
- [Unity Catalog Spark Connector](https://github.com/unitycatalog/unitycatalog)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Implementation Source: PR #15](https://github.com/murali-db/delta/pull/15/files)
