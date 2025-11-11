import org.apache.spark.sql.SparkSession
import sttp.client3._
import io.circe.parser._

/**
 * Test application that demonstrates UC credential injection into DeltaLog.options.
 *
 * Flow:
 * 1. Calls UC Iceberg REST API to get vended S3 credentials
 * 2. Configures SparkSession with CustomDeltaCatalog
 * 3. Passes credentials via spark.sql.catalog.uc.credentials config
 * 4. Loads table (triggers CustomDeltaCatalog.loadTable)
 * 5. Reads data (credentials flow: storage.properties → DeltaLog.options → Hadoop config → executors)
 */
object UCCredentialsTest extends App {

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
  println("UC Credentials Test Application")
  println("=" * 80)

  // Step 1: Call UC Iceberg REST API to get credentials
  println("\n[Step 1] Calling UC Iceberg REST API to vend credentials...")
  val credentials = fetchUCCredentials(UC_URI, UC_TOKEN, CATALOG_NAME, SCHEMA, TABLE)

  println(s"  ✓ Received credentials")
  println(s"    Access Key ID: ${credentials.accessKeyId}")
  println(s"    Expires At: ${new java.util.Date(credentials.expiresAtMs)}")
  println(s"    Region: ${credentials.region}")

  // Step 2: Create SparkSession with CustomDeltaCatalog
  println("\n[Step 2] Creating SparkSession with CustomDeltaCatalog...")
  val spark = SparkSession.builder()
    .appName("UC Credentials Test")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.uc", classOf[CustomDeltaCatalog].getName)
    .config("spark.sql.catalog.uc.credentials", credentials.toJson)
    .config("spark.databricks.delta.loadFileSystemConfigsFromDataFrameOptions", "true")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  println("  ✓ SparkSession created with CustomDeltaCatalog")

  try {
    // Step 3: Create a CatalogTable manually to simulate what UC would do
    println("\n[Step 3] Creating CatalogTable with S3 location...")

    // We'll create a table definition that points to the S3 location
    // In a real UC catalog, this would come from UC metadata service
    println(s"  Table Location: $TABLE_LOCATION")

    // Create table metadata in Spark's catalog
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS uc.${SCHEMA}.${TABLE}
      USING delta
      LOCATION '$TABLE_LOCATION'
    """)

    println(s"  ✓ Table registered: uc.$SCHEMA.$TABLE")

    // Step 4: Load table via catalog (triggers CustomDeltaCatalog.loadTable)
    println("\n[Step 4] Loading table via CustomDeltaCatalog...")
    val table = spark.table(s"uc.$SCHEMA.$TABLE")
    println(s"  ✓ Table loaded successfully")
    println(s"  Schema: ${table.schema.treeString}")

    // Step 5: Read data (triggers file access with UC credentials)
    println("\n[Step 5] Reading table data (using UC-vended credentials)...")
    println("\nTable contents:")
    table.show(truncate = false)

    val count = table.count()
    println(s"\n  ✓ Successfully read $count rows using UC-vended credentials!")

    println("\n" + "=" * 80)
    println("SUCCESS: Credentials flowed correctly!")
    println("  CatalogTable.storage.properties → DeltaLog.options → Hadoop Configuration")
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
