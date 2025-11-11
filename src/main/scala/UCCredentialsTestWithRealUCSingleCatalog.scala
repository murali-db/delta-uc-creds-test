import org.apache.spark.sql.SparkSession

/**
 * Test application demonstrating UC credential injection via real UCSingleCatalog.
 *
 * Flow:
 * 1. Configures SparkSession with real io.unitycatalog.spark.UCSingleCatalog
 * 2. Catalog automatically connects to UC server (no manual REST API calls!)
 * 3. When table is accessed, catalog fetches credentials from UC server
 * 4. Credentials are injected into table storage properties
 * 5. Reads data (credentials flow: storage.properties → DeltaLog.options → Hadoop config → executors)
 *
 * This is the production pattern - the catalog handles all UC communication automatically.
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

  println("=" * 80)
  println("UC Credentials Test Application (with Real UCSingleCatalog)")
  println("Production Pattern - Catalog Handles All UC Communication")
  println("=" * 80)

  // Step 1: Create SparkSession with real UCSingleCatalog
  println("\n[Step 1] Creating SparkSession with real UCSingleCatalog...")
  println(s"  UC Server: $UC_URI")
  println(s"  Catalog: $CATALOG_NAME")

  val spark = SparkSession.builder()
    .appName("UC Credentials Test with Real UCSingleCatalog")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    // Register real UCSingleCatalog as "unity" catalog
    .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog")
    // Configure UC server connection
    .config("spark.sql.catalog.unity.uri", UC_URI)
    .config("spark.sql.catalog.unity.token", UC_TOKEN)
    // Optional: Disable automatic credential renewal for this test
    .config("spark.sql.catalog.unity.renewCredential.enabled", "false")
    .config("spark.databricks.delta.loadFileSystemConfigsFromDataFrameOptions", "true")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  println("  ✓ SparkSession created with real UCSingleCatalog")
  println(s"    Catalog: unity (io.unitycatalog.spark.UCSingleCatalog)")
  println(s"    Pattern: UCSingleCatalog → DeltaCatalog → UCProxy")
  println(s"    UCProxy will fetch credentials from UC server automatically")

  try {
    // Step 2: Read table via catalog
    println("\n[Step 2] Reading table via real UCSingleCatalog...")
    println(s"  Table: unity.$CATALOG_NAME.$SCHEMA.$TABLE")
    println("  This will trigger:")
    println("    1. UCProxy connects to UC server")
    println("    2. Fetches table metadata via TablesApi.getTable()")
    println("    3. Requests temporary credentials via TemporaryCredentialsApi")
    println("    4. Injects credentials into CatalogTable.storage.properties")
    println("    5. Returns table ready for Spark to read\n")

    // Read the table via catalog - UCSingleCatalog handles everything!
    val df = spark.table(s"unity.$CATALOG_NAME.$SCHEMA.$TABLE")

    println("\nTable contents:")
    df.show(truncate = false)

    val count = df.count()
    println(s"\n  ✓ Successfully read $count rows using UCSingleCatalog!")
    println("    Credentials were automatically fetched from UC server")

    println("\n" + "=" * 80)
    println("SUCCESS: Real UCSingleCatalog working correctly!")
    println("  Credential Flow:")
    println("  UCSingleCatalog → UC Server (REST API) → Temporary Credentials →")
    println("  storage.properties → DeltaLog.options → Hadoop Configuration → S3")
    println("\n  This is the production pattern for Unity Catalog integration!")
    println("=" * 80)

  } catch {
    case e: Exception =>
      println("\n" + "=" * 80)
      println("ERROR: Failed to read table via UCSingleCatalog")
      println("=" * 80)
      println("\nPossible issues:")
      println("  1. UC_URI might need to be the Unity Catalog endpoint")
      println("  2. Token might not have permissions for the table")
      println("  3. Catalog/schema/table might not exist in Unity Catalog")
      println("  4. Network connectivity to UC server")
      println("\nFull error:")
      e.printStackTrace()
  } finally {
    spark.stop()
  }
}
