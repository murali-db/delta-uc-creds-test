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

  println("UC Credentials Test: Three Approaches\n")

  // Fetch credentials and create SparkSession
  val credentials = fetchUCCredentials(UC_URI, UC_TOKEN, CATALOG_NAME, SCHEMA, TABLE)
  val spark = SparkSession.builder()
    .appName("UC Credentials Test - Combined Approaches")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config(s"spark.sql.catalog.$CATALOG_NAME", "io.unitycatalog.spark.UCSingleCatalog")
    .config(s"spark.sql.catalog.$CATALOG_NAME.uri", UC_URI)
    .config(s"spark.sql.catalog.$CATALOG_NAME.token", UC_TOKEN)
    .config(s"spark.sql.catalog.$CATALOG_NAME.renewCredential.enabled", "false")
    .config("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.databricks.delta.loadFileSystemConfigsFromDataFrameOptions", "true")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val s3aLocation = TABLE_LOCATION.replace("s3://", "s3a://")

  try {
    // APPROACH 1: Path-based with manual credentials
    println("Testing Approach 1: Path-based with manual /plan endpoint credentials")
    val credentialOptions = Map(
      "fs.s3a.access.key" -> credentials.accessKeyId,
      "fs.s3a.secret.key" -> credentials.secretAccessKey,
      "fs.s3a.session.token" -> credentials.sessionToken
    )
    val dfPathBased = spark.read.format("delta").options(credentialOptions).load(s3aLocation)
    val count1 = dfPathBased.count()
    println(s"→ Table output: $count1 rows. This is expected.\n")

    // APPROACH 2: UCSingleCatalog with automatic credentials
    println("Testing Approach 2: UCSingleCatalog with automatic credential fetching")
    try {
      val dfCatalogBased = spark.table(s"$CATALOG_NAME.$SCHEMA.$TABLE")
      val count2 = dfCatalogBased.count()
      println(s"→ Table output: $count2 rows. This is expected.\n")
    } catch {
      case e: Exception =>
        println(s"→ Unable to read table: ${e.getClass.getSimpleName}. This is not expected.\n")
    }

    // APPROACH 3: CredPropsUtil pattern with credential stripping
    println("Testing Approach 3: CredPropsUtil pattern (simulating future non-vending UC)")
    try {
      val catalog = spark.sessionState.catalogManager.catalog(CATALOG_NAME)
        .asInstanceOf[org.apache.spark.sql.connector.catalog.TableCatalog]
      val ident = Identifier.of(Array(SCHEMA), TABLE)
      val tableWithCreds = catalog.loadTable(ident)
      val catalogTableField = tableWithCreds.getClass.getMethod("catalogTable")
      val catalogTableOpt = catalogTableField.invoke(tableWithCreds).asInstanceOf[Option[CatalogTable]]
      val catalogTable = catalogTableOpt.get
      val tablePath = catalogTable.storage.locationUri.get.toString
      val originalProps = catalogTable.storage.properties

      // Step 3a: Strip credentials
      println("→ Testing with stripped credentials (simulating non-vending UC)")
      val strippedProps = originalProps.filterNot(_._1.startsWith("fs.s3a"))
      try {
        val dfNoCreds = spark.read.format("delta").options(strippedProps).load(tablePath)
        dfNoCreds.count()
        println("  Unable to read table: No error. This is not expected.")
      } catch {
        case e: Exception if e.getMessage.contains("403") || e.getMessage.contains("Access Denied") =>
          println("  Unable to read table: Access Denied. This is expected.")
      }

      // Step 3b: Re-inject credentials using CredPropsUtil
      println("→ Testing with re-injected credentials via CredPropsUtil")
      val planCreds = fetchUCCredentials(UC_URI, UC_TOKEN, CATALOG_NAME, SCHEMA, TABLE)
      val awsCredentials = new AwsCredentials()
        .accessKeyId(planCreds.accessKeyId)
        .secretAccessKey(planCreds.secretAccessKey)
        .sessionToken(planCreds.sessionToken)
      val temporaryCredentials = new TemporaryCredentials().awsTempCredentials(awsCredentials)
      val credProps = CredPropsUtil.createTableCredProps(
        false, "s3", UC_URI, UC_TOKEN,
        s"$CATALOG_NAME.$SCHEMA.$TABLE",
        TableOperation.READ, temporaryCredentials
      ).asScala.toMap
      val mergedProps = strippedProps ++ credProps
      val dfWithCredProps = spark.read.format("delta").options(mergedProps).load(tablePath)
      val countCredProps = dfWithCredProps.count()
      println(s"  Table output: $countCredProps rows. This is expected.\n")
    } catch {
      case e: Exception =>
        println(s"→ Unable to complete Approach 3: ${e.getClass.getSimpleName}. This is not expected.\n")
    }

  } catch {
    case e: Exception =>
      println(s"ERROR: Test failed - ${e.getMessage}")
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
