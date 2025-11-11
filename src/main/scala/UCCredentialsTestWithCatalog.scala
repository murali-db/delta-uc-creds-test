import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier
import sttp.client3._
import io.circe.parser._

/**
 * Test application demonstrating UC credential injection via CustomUCCatalog (UCSingleCatalog pattern).
 *
 * Flow:
 * 1. Calls UC Iceberg REST API to get vended S3 credentials
 * 2. Configures SparkSession with CustomUCCatalog
 * 3. Passes credentials via spark.sql.catalog.uc.credentials config
 * 4. Accesses table via catalog loadTable() method
 * 5. CustomUCCatalog → DeltaCatalog → CustomProxy injects credentials
 * 6. Reads data (credentials flow: storage.properties → DeltaLog.options → Hadoop config → executors)
 */
object UCCredentialsTestWithCatalog extends App {

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
  println("UC Credentials Test Application (with CustomUCCatalog)")
  println("Following UCSingleCatalog Pattern")
  println("=" * 80)

  // Step 1: Call UC Iceberg REST API to get credentials
  println("\n[Step 1] Calling UC Iceberg REST API to vend credentials...")
  val credentials = fetchUCCredentials(UC_URI, UC_TOKEN, CATALOG_NAME, SCHEMA, TABLE)

  println(s"  ✓ Received credentials")
  println(s"    Access Key ID: ${credentials.accessKeyId}")
  println(s"    Expires At: ${new java.util.Date(credentials.expiresAtMs)}")
  println(s"    Region: ${credentials.region}")

  // Step 2: Create SparkSession with CustomUCCatalog
  println("\n[Step 2] Creating SparkSession with CustomUCCatalog...")

  // Convert s3:// to s3a:// for proper S3A filesystem handling
  val s3aLocation = TABLE_LOCATION.replace("s3://", "s3a://")

  val spark = SparkSession.builder()
    .appName("UC Credentials Test with Catalog")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    // Register CustomUCCatalog as "uc" catalog (follows UCSingleCatalog pattern)
    .config("spark.sql.catalog.uc", "CustomUCCatalog")
    // Pass UC credentials to the catalog via credentials option
    .config("spark.sql.catalog.uc.credentials", credentials.toJson)
    // Pass table location to catalog
    .config("spark.sql.catalog.uc.table_location", s3aLocation)
    .config("spark.databricks.delta.loadFileSystemConfigsFromDataFrameOptions", "true")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  println("  ✓ SparkSession created with CustomUCCatalog registered")
  println(s"    Catalog: uc (CustomUCCatalog)")
  println(s"    Pattern: CustomUCCatalog → DeltaCatalog → CustomProxy")
  println(s"    Credentials passed via config")

  try {
    // Step 3: Load table via catalog
    println("\n[Step 3] Loading table via CustomUCCatalog...")
    println(s"  Table Location: $s3aLocation")
    println(s"  This will trigger: CustomUCCatalog → DeltaCatalog → CustomProxy.loadTable()\n")

    // Get the catalog
    val catalog = spark.sessionState.catalogManager.catalog("uc")

    // Create identifier with schema and table name
    val ident = Identifier.of(Array(SCHEMA), TABLE)

    // Load table - this triggers the credential injection chain
    val table = catalog.asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog]
      .loadTable(ident)

    println(s"\n  ✓ Table loaded successfully via catalog")

    // Step 4: Read table via catalog (using catalog-injected credentials)
    println("\n[Step 4] Reading table data via CATALOG (not path-based)...")
    println(s"  Using catalog identifier: uc.$SCHEMA.$TABLE")

    // Read the table via catalog - this should use the V1Table with injected credentials
    val df = spark.table(s"uc.$SCHEMA.$TABLE")

    println("\nTable contents:")
    df.show(truncate = false)

    val count = df.count()
    println(s"\n  ✓ Successfully read $count rows using catalog-injected credentials!")

    println("\n" + "=" * 80)
    println("SUCCESS: Credentials flowed correctly through catalog!")
    println("  Catalog config → CustomUCCatalog → DeltaCatalog → CustomProxy →")
    println("  storage.properties → DeltaLog.options → Hadoop Configuration → S3")
    println("=" * 80)

  } catch {
    case e: Exception =>
      println("\n" + "=" * 80)
      println("ERROR: Failed to read table")
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
