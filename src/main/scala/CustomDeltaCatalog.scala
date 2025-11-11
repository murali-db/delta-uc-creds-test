import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.delta.catalog.{DeltaCatalog, DeltaTableV2}
import org.apache.hadoop.fs.Path
import io.circe.parser._

/**
 * Custom DeltaCatalog that injects UC-vended credentials into table storage properties.
 *
 * This catalog intercepts loadTable() calls and injects S3 credentials from UC
 * into CatalogTable.storage.properties, which then flow to DeltaLog.options and
 * eventually to Hadoop Configuration for executor file access.
 */
class CustomDeltaCatalog extends DeltaCatalog {

  override def loadTable(ident: Identifier): Table = {
    // Get DeltaTableV2 from parent catalog
    val table = super.loadTable(ident).asInstanceOf[DeltaTableV2]

    // Check if we have UC credentials configured
    val maybeCredsJson = try {
      Some(spark.conf.get("spark.sql.catalog.uc.credentials"))
    } catch {
      case _: NoSuchElementException => None
    }

    maybeCredsJson match {
      case Some(credsJson) =>
        // Parse credentials JSON
        val creds = parseCredentials(credsJson)

        // Create storage properties with UC credentials
        val ucStorageProps = Map(
          "fs.s3a.access.key" -> creds.accessKeyId,
          "fs.s3a.secret.key" -> creds.secretAccessKey,
          "fs.s3a.session.token" -> creds.sessionToken,
          "fs.s3a.path.style.access" -> "true",
          "fs.s3.impl.disable.cache" -> "true",
          "fs.s3a.impl.disable.cache" -> "true"
        )

        println(s"[CustomDeltaCatalog] Injecting UC credentials for table: ${ident.toString}")
        println(s"[CustomDeltaCatalog] Access Key ID: ${creds.accessKeyId}")
        println(s"[CustomDeltaCatalog] Expires At: ${creds.expiresAtMs}")

        // Update catalogTable with credentials in storage.properties
        val updatedCatalogTable = table.catalogTable.map { ct =>
          ct.copy(
            storage = ct.storage.copy(
              properties = ct.storage.properties ++ ucStorageProps
            )
          )
        }

        // Create new DeltaTableV2 with updated catalogTable
        DeltaTableV2(
          spark = table.spark,
          path = table.path,
          catalogTable = updatedCatalogTable,
          tableIdentifier = table.tableIdentifier,
          timeTravelOpt = table.timeTravelOpt,
          options = table.options,
          cdcOptions = table.cdcOptions
        )

      case None =>
        // No credentials configured, return table as-is
        println(s"[CustomDeltaCatalog] No UC credentials configured, using default")
        table
    }
  }

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
