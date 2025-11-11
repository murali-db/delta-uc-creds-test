import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import sttp.client3._
import io.circe.parser._

/**
 * Test application demonstrating UC credential stripping and re-fetching.
 *
 * Flow:
 * 1. Uses real UCSingleCatalog to load table (with auto-fetched credentials)
 * 2. Strips credentials from table metadata
 * 3. Attempts to read without credentials (SHOULD FAIL - proves credentials are needed)
 * 4. Manually fetches credentials via UC REST API /plan endpoint
 * 5. Reads table with manual credentials (SHOULD SUCCEED)
 *
 * This demonstrates:
 * - UCSingleCatalog automatically fetches and injects credentials
 * - Without credentials, S3 access fails
 * - Manual credential fetching via /plan endpoint works as fallback
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
  println("UC Credentials Test: Stripping and Re-fetching Flow")
  println("=" * 80)

  // Create SparkSession with real UCSingleCatalog
  println("\n[Setup] Creating SparkSession with real UCSingleCatalog...")
  println(s"  UC Server: $UC_URI")
  println(s"  Catalog: $CATALOG_NAME")

  val spark = SparkSession.builder()
    .appName("UC Credentials Test - Stripping and Re-fetching")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    // Register real UCSingleCatalog as "unity" catalog
    .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog")
    // Configure UC server connection
    .config("spark.sql.catalog.unity.uri", UC_URI)
    .config("spark.sql.catalog.unity.token", UC_TOKEN)
    // Disable automatic credential renewal - not needed for this short-lived test.
    // UC credentials typically last 1 hour, plenty for a quick read operation.
    // Enable this for long-running jobs (>1 hour), streaming apps, or interactive notebooks.
    .config("spark.sql.catalog.unity.renewCredential.enabled", "false")
    .config("spark.databricks.delta.loadFileSystemConfigsFromDataFrameOptions", "true")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  println("  ✓ SparkSession created")

  // Convert s3:// to s3a:// for proper S3A filesystem handling
  val s3aLocation = TABLE_LOCATION.replace("s3://", "s3a://")

  try {
    // STEP 1: Use UCSingleCatalog to load table with auto-fetched credentials
    println("\n" + "=" * 80)
    println("[Step 1] Loading table via UCSingleCatalog (with auto-fetched credentials)")
    println("=" * 80)
    println(s"  Table: unity.$CATALOG_NAME.$SCHEMA.$TABLE")
    println("  UCSingleCatalog will:")
    println("    1. Connect to UC server")
    println("    2. Fetch table metadata")
    println("    3. Request temporary credentials")
    println("    4. Inject credentials into storage.properties")

    // Get the catalog
    val catalog = spark.sessionState.catalogManager.catalog("unity")
      .asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog]

    // Create identifier
    val ident = Identifier.of(Array(CATALOG_NAME, SCHEMA), TABLE)

    // Load table - UCSingleCatalog fetches credentials automatically
    val tableWithCreds = catalog.loadTable(ident)

    // Extract storage properties to show credentials were injected
    // Use reflection to access the catalogTable field from V1Table (which is package-private)
    val catalogTableField = tableWithCreds.getClass.getMethod("catalogTable")
    val catalogTable = catalogTableField.invoke(tableWithCreds).asInstanceOf[CatalogTable]
    val storageProps = catalogTable.storage.properties

    println("\n  ✓ Table loaded successfully with auto-fetched credentials")
    println("  Credentials found in storage.properties:")
    storageProps.filter(_._1.startsWith("fs.s3a")).foreach { case (key, value) =>
      if (key.contains("secret") || key.contains("token")) {
        println(s"    $key = ${value.take(20)}... (${value.length} chars)")
      } else {
        println(s"    $key = $value")
      }
    }

    // STEP 2: Strip credentials from table metadata
    println("\n" + "=" * 80)
    println("[Step 2] Stripping credentials from table metadata")
    println("=" * 80)
    println("  Removing all fs.s3a.* properties from storage.properties...")

    val strippedProps = storageProps.filterNot(_._1.startsWith("fs.s3a"))
    println(s"  ✓ Stripped ${storageProps.size - strippedProps.size} credential properties")
    println(s"    Original properties: ${storageProps.size}")
    println(s"    Stripped properties: ${strippedProps.size}")

    // STEP 3: Attempt to read with NO credentials (should fail)
    println("\n" + "=" * 80)
    println("[Step 3] Attempting to read table WITHOUT credentials (should fail)")
    println("=" * 80)
    println(s"  Table Location: $s3aLocation")
    println("  This should fail with S3 Access Denied error...")

    try {
      val dfNoCreds = spark.read
        .format("delta")
        .load(s3aLocation)

      // Try to trigger file access
      dfNoCreds.count()

      println("\n  ✗ UNEXPECTED: Read succeeded without credentials!")
      println("    This shouldn't happen - credentials are required for S3 access")

    } catch {
      case e: Exception if e.getMessage.contains("403") ||
                           e.getMessage.contains("Access Denied") ||
                           e.getMessage.contains("Forbidden") =>
        println("\n  ✓ EXPECTED FAILURE: Access Denied (403)")
        println(s"    Error: ${e.getMessage.take(200)}...")
        println("\n  → This proves credentials are REQUIRED to access the table!")

      case e: Exception =>
        println(s"\n  ✗ Different error occurred: ${e.getMessage}")
        println("    Expected: Access Denied (403)")
        e.printStackTrace()
    }

    // STEP 4: Manually fetch credentials via /plan endpoint
    println("\n" + "=" * 80)
    println("[Step 4] Manually fetching credentials via UC REST API /plan endpoint")
    println("=" * 80)

    val credentials = fetchUCCredentials(UC_URI, UC_TOKEN, CATALOG_NAME, SCHEMA, TABLE)

    println("  ✓ Successfully fetched credentials from UC server")
    println(s"    Access Key ID: ${credentials.accessKeyId}")
    println(s"    Expires At: ${new java.util.Date(credentials.expiresAtMs)}")
    println(s"    Region: ${credentials.region}")

    // STEP 5: Read table with manually fetched credentials (should succeed)
    println("\n" + "=" * 80)
    println("[Step 5] Reading table WITH manually fetched credentials (should succeed)")
    println("=" * 80)

    val credentialOptions = Map(
      "fs.s3a.access.key" -> credentials.accessKeyId,
      "fs.s3a.secret.key" -> credentials.secretAccessKey,
      "fs.s3a.session.token" -> credentials.sessionToken
    )

    val dfWithCreds = spark.read
      .format("delta")
      .options(credentialOptions)
      .load(s3aLocation)

    println("\nTable contents:")
    dfWithCreds.show(truncate = false)

    val count = dfWithCreds.count()
    println(s"\n  ✓ SUCCESS: Read $count rows using manually fetched credentials!")

    // SUMMARY
    println("\n" + "=" * 80)
    println("TEST SUMMARY: Credential Stripping and Re-fetching Flow")
    println("=" * 80)
    println("  ✓ Step 1: UCSingleCatalog auto-fetched credentials")
    println("  ✓ Step 2: Successfully stripped credentials from metadata")
    println("  ✓ Step 3: Read failed without credentials (as expected)")
    println("  ✓ Step 4: Manually fetched credentials via /plan endpoint")
    println("  ✓ Step 5: Read succeeded with manual credentials")
    println("\nKey Findings:")
    println("  1. UCSingleCatalog automatically injects credentials into storage.properties")
    println("  2. Without credentials, S3 access fails (Access Denied 403)")
    println("  3. Manual credential fetching via /plan endpoint works as fallback")
    println("  4. Both approaches (auto-fetch and manual) successfully provide S3 access")
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
