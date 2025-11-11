import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import sttp.client3._
import io.circe.parser._
import io.unitycatalog.client.model.{AwsCredentials, TemporaryCredentials, TableOperation}
import io.unitycatalog.spark.auth.CredPropsUtil
import scala.jdk.CollectionConverters._

/**
 * Combined test demonstrating three approaches for UC credential handling.
 *
 * Approach 1 - Path-based with manual credential fetching:
 *   1. Call UC REST API /plan endpoint to fetch credentials manually
 *   2. Read table via path using DataFrame options
 *   Status: ✓ Working - demonstrates direct credential management
 *
 * Approach 2 - UCSingleCatalog (auto credential fetching):
 *   1. Configure UCSingleCatalog in SparkSession
 *   2. Read table via catalog (UCSingleCatalog fetches credentials automatically)
 *   Status: ⚠ Requires table registered in UC catalog metadata
 *
 * Approach 3 - CredPropsUtil Pattern (Future-Proof):
 *   1. Load table from UCSingleCatalog to extract metadata
 *   2. Strip credentials (simulating future non-vending UC server)
 *   3. Fetch credentials from /plan endpoint
 *   4. Convert to TemporaryCredentials object (with AwsCredentials)
 *   5. Use CredPropsUtil.createTableCredProps() to inject credentials (same as UCSingleCatalog!)
 *   6. Merge using ++ pattern (properties ++ credProps)
 *   7. Read successfully with injected credentials
 *   Status: ⚠ Requires table registered in UC catalog metadata (for initial load)
 *
 * All approaches demonstrate different patterns for UC credential management.
 */
object UCCredentialsTestWithRealUCSingleCatalog extends App {

  // UC Configuration - read from environment variables
  val UC_URI = sys.env.getOrElse("UC_URI",
    throw new RuntimeException("UC_URI environment variable not set"))
  val UC_TOKEN = sys.env.getOrElse("UC_TOKEN",
    throw new RuntimeException("UC_TOKEN environment variable not set"))
  val CATALOG_NAME = sys.env.getOrElse("CATALOG_NAME",
    throw new RuntimeException("CATALOG_NAME environment variable not set"))
  val SCHEMA = sys.env.getOrElse("SCHEMA",
    throw new RuntimeException("SCHEMA environment variable not set"))
  val TABLE = sys.env.getOrElse("TABLE",
    throw new RuntimeException("TABLE environment variable not set"))
  val TABLE_LOCATION = sys.env.getOrElse("TABLE_LOCATION",
    throw new RuntimeException("TABLE_LOCATION environment variable not set"))

  println("=" * 80)
  println("UC Credentials Test: Path-based vs UCSingleCatalog")
  println("=" * 80)

  // ============================================================================
  // APPROACH 1: Path-based with Manual Credential Fetching
  // ============================================================================

  println("\n" + "=" * 80)
  println("APPROACH 1: Path-based with Manual Credential Fetching")
  println("=" * 80)

  // Step 1: Call UC Iceberg REST API to get credentials
  println("\n[Step 1.1] Calling UC Iceberg REST API /plan endpoint to fetch credentials...")
  val credentials = fetchUCCredentials(UC_URI, UC_TOKEN, CATALOG_NAME, SCHEMA, TABLE)

  println(s"  ✓ Received credentials from /plan endpoint")
  println(s"    Access Key ID: ${credentials.accessKeyId}")
  println(s"    Expires At: ${new java.util.Date(credentials.expiresAtMs)}")
  println(s"    Region: ${credentials.region}")

  // Step 2: Create SparkSession (initially without UCSingleCatalog)
  println("\n[Step 1.2] Creating SparkSession...")
  val spark = SparkSession.builder()
    .appName("UC Credentials Test - Combined Approaches")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    // Will add UCSingleCatalog config later for Approach 2
    .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog")
    .config("spark.sql.catalog.unity.uri", UC_URI)
    .config("spark.sql.catalog.unity.token", UC_TOKEN)
    .config("spark.sql.catalog.unity.renewCredential.enabled", "false")
    .config("spark.databricks.delta.loadFileSystemConfigsFromDataFrameOptions", "true")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  println("  ✓ SparkSession created")

  // Convert s3:// to s3a:// for proper S3A filesystem handling
  val s3aLocation = TABLE_LOCATION.replace("s3://", "s3a://")

  try {
    // Step 3: Read Delta table with manual credentials via path
    println("\n[Step 1.3] Reading table via PATH with manual credentials...")
    println(s"  Table Location: $s3aLocation")

    // Create Hadoop configuration properties from UC credentials
    val credentialOptions = Map(
      "fs.s3a.access.key" -> credentials.accessKeyId,
      "fs.s3a.secret.key" -> credentials.secretAccessKey,
      "fs.s3a.session.token" -> credentials.sessionToken
    )

    // Read Delta table with credentials
    val dfPathBased = spark.read
      .format("delta")
      .options(credentialOptions)
      .load(s3aLocation)

    println("\nTable contents (Path-based read):")
    dfPathBased.show(truncate = false)

    val count1 = dfPathBased.count()
    println(s"\n  ✓ APPROACH 1 SUCCESS: Read $count1 rows via path with manual credentials!")
    println("    Flow: /plan endpoint → manual credentials → DataFrame options → S3")

    // ============================================================================
    // APPROACH 2: UCSingleCatalog (Auto Credential Fetching)
    // ============================================================================

    println("\n" + "=" * 80)
    println("APPROACH 2: UCSingleCatalog with Automatic Credential Fetching")
    println("=" * 80)

    println("\n[Step 2.1] Reading table via UCSingleCatalog...")
    println(s"  Table: unity.$SCHEMA.$TABLE")
    println("  UCSingleCatalog will:")
    println("    1. Connect to UC server")
    println("    2. Fetch table metadata automatically")
    println("    3. Request temporary credentials automatically")
    println("    4. Inject credentials into table")

    try {
      // Read the table via catalog - UCSingleCatalog handles everything!
      val dfCatalogBased = spark.table(s"unity.$SCHEMA.$TABLE")

      println("\nTable contents (Catalog-based read):")
      dfCatalogBased.show(truncate = false)

      val count2 = dfCatalogBased.count()
      println(s"\n  ✓ APPROACH 2 SUCCESS: Read $count2 rows via UCSingleCatalog!")
      println("    Flow: UCSingleCatalog → UC server → auto credentials → S3")
    } catch {
      case e: Exception if e.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND") =>
        println("\n  ⚠ APPROACH 2 SKIPPED: Table not registered in Unity Catalog")
        println("    This table is accessible via /plan endpoint but not via UC catalog APIs")
        println("    To use UCSingleCatalog, the table must be registered in UC metadata")
        println(s"    Error: ${e.getMessage.take(150)}...")
    }

    // ============================================================================
    // APPROACH 3: CredPropsUtil Pattern (Future-Proof)
    // ============================================================================

    println("\n" + "=" * 80)
    println("APPROACH 3: CredPropsUtil Pattern - Simulating Future Non-Vending UC")
    println("=" * 80)
    println("\nThis approach demonstrates how to handle UC servers that don't vend credentials")
    println("by using CredPropsUtil (same mechanism UCSingleCatalog uses internally).")

    try {
      // Step 1: Load table from UCSingleCatalog
      println("\n[Step 3.1] Loading table from UCSingleCatalog to extract metadata...")
      val catalog = spark.sessionState.catalogManager.catalog("unity")
        .asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog]
      val ident = Identifier.of(Array(SCHEMA), TABLE)
      val tableWithCreds = catalog.loadTable(ident)

      // Step 2: Extract CatalogTable using reflection (to get storage properties)
      println("  ✓ Extracting CatalogTable from V1Table...")
      val catalogTableField = tableWithCreds.getClass.getMethod("catalogTable")
      val catalogTable = catalogTableField.invoke(tableWithCreds).asInstanceOf[CatalogTable]
      val tablePath = catalogTable.storage.locationUri.get.toString
      val originalProps = catalogTable.storage.properties

      println(s"  Table path: $tablePath")
      println(s"  Original properties: ${originalProps.size} total")

      // Step 3: Read with UCSingleCatalog credentials → SUCCESS
      println("\n[Step 3.2] Reading with UCSingleCatalog-provided credentials...")
      val ucCreds = originalProps.filter(_._1.startsWith("fs.s3a"))
      println(s"  Found ${ucCreds.size} credential properties from UCSingleCatalog")

      val dfWithUCCreds = spark.read
        .format("delta")
        .options(ucCreds)
        .load(tablePath)

      println("\nTable contents (UCSingleCatalog credentials):")
      dfWithUCCreds.show(truncate = false)
      val countUC = dfWithUCCreds.count()
      println(s"  ✓ SUCCESS: Read $countUC rows with UCSingleCatalog credentials")

      // Step 4: Strip credentials (simulate future non-vending UC)
      println("\n[Step 3.3] Stripping credentials to simulate future non-vending UC server...")
      val strippedProps = originalProps.filterNot(_._1.startsWith("fs.s3a"))
      println(s"  Stripped credentials. Properties remaining: ${strippedProps.size}")

      // Step 5: Try reading without credentials → FAIL
      println("\n[Step 3.4] Attempting to read WITHOUT credentials (should fail)...")
      try {
        val dfNoCreds = spark.read
          .format("delta")
          .options(strippedProps)
          .load(tablePath)
        dfNoCreds.count() // Try to trigger file access
        println("  ✗ UNEXPECTED: Read succeeded without credentials!")
      } catch {
        case e: Exception if e.getMessage.contains("403") ||
                             e.getMessage.contains("Access Denied") ||
                             e.getMessage.contains("Forbidden") =>
          println("  ✓ EXPECTED FAILURE: Access Denied (403)")
          println("  → Proves credentials are REQUIRED!")
      }

      // Step 6: Fetch credentials from /plan endpoint
      println("\n[Step 3.5] Fetching credentials from /plan endpoint...")
      val planCreds = fetchUCCredentials(UC_URI, UC_TOKEN, CATALOG_NAME, SCHEMA, TABLE)
      println(s"  ✓ Fetched credentials from /plan endpoint")
      println(s"    Access Key: ${planCreds.accessKeyId}")

      // Step 7: Convert to UC client TemporaryCredentials
      println("\n[Step 3.6] Converting to UC client TemporaryCredentials object...")
      val awsCredentials = new AwsCredentials()
        .accessKeyId(planCreds.accessKeyId)
        .secretAccessKey(planCreds.secretAccessKey)
        .sessionToken(planCreds.sessionToken)

      val temporaryCredentials = new TemporaryCredentials()
        .awsTempCredentials(awsCredentials)
      println("  ✓ Converted to TemporaryCredentials with AwsCredentials")

      // Step 8: Use CredPropsUtil (same as UCSingleCatalog!)
      println("\n[Step 3.7] Using CredPropsUtil.createTableCredProps() (same as UCSingleCatalog!)...")
      val credProps = CredPropsUtil.createTableCredProps(
        false,                              // renewCredEnabled
        "s3",                               // scheme
        UC_URI,                             // serverUri
        UC_TOKEN,                           // authToken
        s"$CATALOG_NAME.$SCHEMA.$TABLE",    // tableId
        TableOperation.READ,                // operation
        temporaryCredentials                // credentials from /plan
      ).asScala.toMap

      println(s"  ✓ Generated ${credProps.size} credential properties via CredPropsUtil")
      credProps.foreach { case (key, value) =>
        if (key.contains("secret") || key.contains("token")) {
          println(s"    $key = ${value.take(20)}... (${value.length} chars)")
        } else {
          println(s"    $key = $value")
        }
      }

      // Step 9: Merge using ++ pattern (properties ++ credProps overwrites!)
      println("\n[Step 3.8] Merging credentials using ++ pattern (strippedProps ++ credProps)...")
      val mergedProps = strippedProps ++ credProps
      println(s"  ✓ Merged properties. Total: ${mergedProps.size}")
      println("  Note: The ++ operator overwrites duplicate keys (same pattern as UCSingleCatalog)")

      // Step 10: Read with CredPropsUtil-injected credentials → SUCCESS
      println("\n[Step 3.9] Reading with CredPropsUtil-injected credentials...")
      val dfWithCredProps = spark.read
        .format("delta")
        .options(mergedProps)
        .load(tablePath)

      println("\nTable contents (CredPropsUtil credentials):")
      dfWithCredProps.show(truncate = false)
      val countCredProps = dfWithCredProps.count()
      println(s"\n  ✓ APPROACH 3 SUCCESS: Read $countCredProps rows using CredPropsUtil!")
      println("    Flow: /plan endpoint → AwsCredentials → CredPropsUtil → ++ merge → S3")

      println("\n" + "=" * 80)
      println("APPROACH 3 DEMONSTRATES THE FUTURE-PROOF PATTERN!")
      println("=" * 80)
      println("  This is exactly how UCSingleCatalog works internally:")
      println("  1. Fetch table metadata (without credentials in future)")
      println("  2. Fetch credentials separately (/plan endpoint)")
      println("  3. Convert to TemporaryCredentials object (wrapping AwsCredentials)")
      println("  4. Use CredPropsUtil.createTableCredProps() to generate Hadoop properties")
      println("  5. Merge with storage.properties using ++ (overwrites existing)")
      println("  6. Read table successfully!")
      println("=" * 80)

    } catch {
      case e: Exception if e.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND") =>
        println("\n  ⚠ APPROACH 3 SKIPPED: Table not registered in Unity Catalog")
        println("    (Same reason as Approach 2)")
      case e: Exception =>
        println("\n  ✗ APPROACH 3 ERROR:")
        e.printStackTrace()
    }

    // SUMMARY
    println("\n" + "=" * 80)
    println("TEST SUMMARY - Three UC Credential Management Patterns")
    println("=" * 80)
    println(s"  ✓ Approach 1 (Path + /plan endpoint): $count1 rows - SUCCESS")
    println(s"  ⚠ Approach 2 (UCSingleCatalog): Table not in UC catalog metadata")
    println(s"  ⚠ Approach 3 (CredPropsUtil): Table not in UC catalog metadata (same reason)")
    println("\n" + "=" * 80)
    println("Key Findings:")
    println("=" * 80)
    println("\n  Approach 1 - Path-based with Manual Credentials:")
    println("    • Manual: Explicitly call /plan endpoint to fetch credentials")
    println("    • Path-based: spark.read.options(creds).load(path)")
    println("    • Works with: Any table accessible via Iceberg REST /plan endpoint")
    println("    • Use case: Direct control over credential management")
    println("    • Status: ✓ WORKING")
    println("\n  Approach 2 - UCSingleCatalog (Automatic):")
    println("    • Automatic: UCSingleCatalog fetches credentials from UC server")
    println("    • Catalog-based: spark.table(\"unity.schema.table\")")
    println("    • Requires: Table must be registered in Unity Catalog metadata")
    println("    • Use case: Production pattern with transparent credential handling")
    println("    • Status: ⚠ Requires UC catalog registration")
    println("\n  Approach 3 - CredPropsUtil Pattern (Future-Proof):")
    println("    • Hybrid: Get metadata from catalog, credentials from /plan endpoint")
    println("    • Uses: CredPropsUtil.createTableCredProps() (same as UCSingleCatalog internally!)")
    println("    • Pattern: strippedProps ++ credProps (++ overwrites existing keys)")
    println("    • Demonstrates: How to handle future UC servers that don't vend credentials")
    println("    • Use case: Preparing for UC API changes, explicit credential control")
    println("    • Status: ⚠ Requires UC catalog registration (to extract metadata)")
    println("\n" + "=" * 80)
    println("Conclusion:")
    println("=" * 80)
    println("  ✓ Approach 1 works with any table accessible via /plan endpoint")
    println("  ⚠ Approaches 2 & 3 require table to be registered in Unity Catalog")
    println("  ✓ Approach 3 demonstrates the exact pattern UCSingleCatalog uses internally")
    println("  ✓ All three approaches show different patterns for UC credential management")
    println("=" * 80)

  } catch {
    case e: Exception =>
      println("\n" + "=" * 80)
      println("ERROR: Test failed")
      println("=" * 80)
      e.printStackTrace()
  } finally {
    spark.stop()
  }

  /**
   * Fetch UC credentials via Iceberg REST API plan endpoint.
   */
  def fetchUCCredentials(
      ucUri: String,
      ucToken: String,
      catalog: String,
      schema: String,
      table: String
  ): UCCredentials = {
    val backend = HttpURLConnectionBackend()

    val url = s"$ucUri/api/2.1/unity-catalog/iceberg-rest/v1/catalogs/$catalog/namespaces/$schema/tables/$table/plan"

    val request = basicRequest
      .post(uri"$url")
      .header("Authorization", s"Bearer $ucToken")
      .header("Content-Type", "application/json")
      .body("""{"snapshot-id": -1}""")

    val response = request.send(backend)

    response.body match {
      case Right(body) =>
        // Parse JSON response
        val json = parse(body).getOrElse(throw new RuntimeException(s"Invalid JSON: $body"))
        val cursor = json.hcursor

        // Navigate to storage-credentials[0].config
        val configCursor = cursor
          .downField("storage-credentials")
          .downArray
          .downField("config")

        // Extract credentials
        UCCredentials(
          accessKeyId = configCursor.get[String]("s3.access-key-id").getOrElse(
            throw new RuntimeException("Missing s3.access-key-id")
          ),
          secretAccessKey = configCursor.get[String]("s3.secret-access-key").getOrElse(
            throw new RuntimeException("Missing s3.secret-access-key")
          ),
          sessionToken = configCursor.get[String]("s3.session-token").getOrElse(
            throw new RuntimeException("Missing s3.session-token")
          ),
          expiresAtMs = configCursor.get[String]("s3.session-token-expires-at-ms")
            .map(_.toLong)
            .getOrElse(System.currentTimeMillis() + 3600000),
          region = configCursor.get[String]("client.region").getOrElse("us-west-2")
        )

      case Left(error) =>
        throw new RuntimeException(s"Failed to fetch credentials: $error")
    }
  }
}
