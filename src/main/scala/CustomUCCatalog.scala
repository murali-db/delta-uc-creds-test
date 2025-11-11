import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.hadoop.fs.Path
import io.circe.parser._
import java.util

/**
 * Custom Unity Catalog implementation that injects UC-vended credentials into table storage properties.
 *
 * This catalog follows the UCSingleCatalog pattern:
 * - Implements TableCatalog directly (does NOT extend DeltaCatalog)
 * - Creates DeltaCatalog as a delegate
 * - Sets DeltaCatalog's delegate to an internal proxy
 * - Proxy injects credentials into CatalogTable.storage.properties
 *
 * Flow: CustomUCCatalog → DeltaCatalog → CustomProxy → credentials injected
 */
class CustomUCCatalog extends TableCatalog with SupportsNamespaces {

  @volatile private var delegate: TableCatalog = null
  private var catalogOptions: CaseInsensitiveStringMap = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogOptions = options

    // Create internal proxy that will inject credentials
    val proxy = new CustomProxy(options)
    proxy.initialize(name, options)

    // Create DeltaCatalog instance
    try {
      delegate = Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getDeclaredConstructor().newInstance().asInstanceOf[TableCatalog]

      // Set DeltaCatalog's delegate to our proxy (this is the key!)
      delegate.asInstanceOf[DelegatingCatalogExtension].setDelegateCatalog(proxy)

      println(s"[CustomUCCatalog] Initialized with DeltaCatalog delegate chain")
      println(s"[CustomUCCatalog] Flow: CustomUCCatalog → DeltaCatalog → CustomProxy")
    } catch {
      case e: ClassNotFoundException =>
        println(s"[CustomUCCatalog] DeltaCatalog not found, using proxy directly")
        delegate = proxy
    }
  }

  override def name(): String = delegate.name()

  override def listTables(namespace: Array[String]): Array[Identifier] =
    delegate.listTables(namespace)

  override def loadTable(ident: Identifier): Table = {
    println(s"[CustomUCCatalog] loadTable called for: ${ident.toString}")
    delegate.loadTable(ident)
  }

  override def tableExists(ident: Identifier): Boolean =
    delegate.tableExists(ident)

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    delegate.createTable(ident, schema, partitions, properties)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    delegate.alterTable(ident, changes: _*)
  }

  override def dropTable(ident: Identifier): Boolean =
    delegate.dropTable(ident)

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    delegate.renameTable(oldIdent, newIdent)
  }

  // SupportsNamespaces methods
  override def listNamespaces(): Array[Array[String]] = {
    delegate.asInstanceOf[SupportsNamespaces].listNamespaces()
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    delegate.asInstanceOf[SupportsNamespaces].listNamespaces(namespace)
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    delegate.asInstanceOf[SupportsNamespaces].loadNamespaceMetadata(namespace)
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    delegate.asInstanceOf[SupportsNamespaces].createNamespace(namespace, metadata)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    delegate.asInstanceOf[SupportsNamespaces].alterNamespace(namespace, changes: _*)
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    delegate.asInstanceOf[SupportsNamespaces].dropNamespace(namespace, cascade)
  }
}

/**
 * Internal proxy that handles table loading and credential injection.
 * This is where the actual credential magic happens!
 */
private class CustomProxy(options: CaseInsensitiveStringMap)
    extends TableCatalog with SupportsNamespaces {

  private var catalogName: String = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    println(s"[CustomProxy] Initialized")
  }

  override def name(): String = {
    assert(this.catalogName != null)
    this.catalogName
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    // For this demo, return empty - we're loading tables by path
    Array.empty
  }

  override def loadTable(ident: Identifier): Table = {
    println(s"[CustomProxy] loadTable called for: ${ident.toString}")

    // Extract credentials from catalog options
    val maybeCredsJson = Option(options.get("credentials"))

    maybeCredsJson match {
      case Some(credsJson) =>
        // Parse credentials
        val creds = parseCredentials(credsJson)

        println(s"[CustomProxy] Injecting UC credentials for table: ${ident.toString}")
        println(s"[CustomProxy] Access Key ID: ${creds.accessKeyId}")
        println(s"[CustomProxy] Expires At: ${creds.expiresAtMs}")

        // Create storage properties with UC credentials
        val ucStorageProps = Map(
          "fs.s3a.access.key" -> creds.accessKeyId,
          "fs.s3a.secret.key" -> creds.secretAccessKey,
          "fs.s3a.session.token" -> creds.sessionToken,
          "fs.s3a.path.style.access" -> "true",
          "fs.s3.impl.disable.cache" -> "true",
          "fs.s3a.impl.disable.cache" -> "true"
        )

        // Get table location from catalog options
        val tablePath = Option(options.get("table_location")).getOrElse(
          throw new RuntimeException("table_location not configured in catalog options")
        )

        println(s"[CustomProxy] Table path: $tablePath")
        println(s"[CustomProxy] Credentials injected into storage.properties")

        // Create CatalogTable with credentials in storage.properties
        val locationUri = CatalogUtils.stringToURI(tablePath)
        val identifier = TableIdentifier(ident.name(), ident.namespace().headOption)

        val catalogTable = CatalogTable(
          identifier = identifier,
          tableType = CatalogTableType.EXTERNAL,
          storage = CatalogStorageFormat.empty.copy(
            locationUri = Some(locationUri),
            properties = ucStorageProps  // CREDENTIALS HERE!
          ),
          schema = StructType(Seq.empty),  // Schema will be inferred by Delta
          provider = Some("delta")
        )

        // Return V1Table wrapping the CatalogTable
        // This is what Delta needs to process the table with credentials
        Class.forName("org.apache.spark.sql.connector.catalog.V1Table")
          .getDeclaredConstructor(classOf[CatalogTable])
          .newInstance(catalogTable)
          .asInstanceOf[Table]

      case None =>
        println(s"[CustomProxy] No credentials found, returning empty table")
        throw new RuntimeException("No credentials configured in catalog options")
    }
  }

  override def tableExists(ident: Identifier): Boolean = true

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: util.Map[String, String]): Table = {
    throw new UnsupportedOperationException("createTable not supported in demo")
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("alterTable not supported in demo")
  }

  override def dropTable(ident: Identifier): Boolean = {
    throw new UnsupportedOperationException("dropTable not supported in demo")
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("renameTable not supported in demo")
  }

  // SupportsNamespaces methods
  override def listNamespaces(): Array[Array[String]] = Array.empty

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = Array.empty

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    new util.HashMap[String, String]()
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = {
    // Allow namespace creation for demo
    println(s"[CustomProxy] Namespace created: ${namespace.mkString(".")}")
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    throw new UnsupportedOperationException("alterNamespace not supported in demo")
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = false

  private def parseCredentials(json: String): UCCredentials = {
    val parsed = parse(json).getOrElse(throw new RuntimeException(s"Invalid JSON: $json"))
    val cursor = parsed.hcursor

    UCCredentials(
      accessKeyId = cursor.get[String]("access-key-id").getOrElse(""),
      secretAccessKey = cursor.get[String]("secret-access-key").getOrElse(""),
      sessionToken = cursor.get[String]("session-token").getOrElse(""),
      expiresAtMs = cursor.get[Long]("expires-at-ms").getOrElse(0L),
      region = cursor.get[String]("region").getOrElse("us-west-2")
    )
  }
}

case class UCCredentials(
  accessKeyId: String,
  secretAccessKey: String,
  sessionToken: String,
  expiresAtMs: Long,
  region: String
) {
  def toJson: String = {
    s"""{
       |  "access-key-id": "$accessKeyId",
       |  "secret-access-key": "$secretAccessKey",
       |  "session-token": "$sessionToken",
       |  "expires-at-ms": $expiresAtMs,
       |  "region": "$region"
       |}""".stripMargin
  }
}
