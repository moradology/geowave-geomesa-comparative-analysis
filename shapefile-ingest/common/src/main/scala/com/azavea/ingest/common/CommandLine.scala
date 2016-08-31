package com.azavea.ingest.common

import java.util.HashMap
import scala.collection.JavaConversions._


object CommandLine {

  sealed trait WaveOrMesa
  case object GeoWave extends WaveOrMesa
  case object GeoMesa extends WaveOrMesa

  implicit val waveOrMesaRead: scopt.Read[WaveOrMesa] =
    scopt.Read.reads(_.toLowerCase match {
        case "geomesa" => GeoMesa
        case "geowave" => GeoWave
        case _ => throw new IllegalArgumentException("Expected 'geowave' or 'geomesa'")
    })

  sealed trait FileFormat
  case object CSV extends FileFormat
  case object SHP extends FileFormat

  implicit val FileFormatRead: scopt.Read[FileFormat] =
    scopt.Read.reads(_.toLowerCase match {
        case "csv" => CSV
        case "shp" => SHP
        case "shapefile" => SHP
        case _ => throw new IllegalArgumentException("Expected 'geowave' or 'geomesa'")
    })

  case class Params (waveOrMesa: WaveOrMesa = GeoMesa,
                     fileFormat: FileFormat = CSV,
                     instanceId: String = "instance",
                     zookeepers: String = "zookeeper",
                     user: String = "root",
                     password: String = "GisPwd",
                     tableName: String = "",
                     s3bucket: String = "",
                     s3prefix: String = "") {

    def convertToJMap(): HashMap[String, String] = {
      val result = new HashMap[String, String]
      result.put("instanceId", instanceId)
      result.put("zookeepers", zookeepers)
      result.put("user", user)
      result.put("password", password)
      //result.put("auths", "")
      //result.put("visibilities", "")
      result.put("tableName", tableName)
      result
    }
  }

  val parser = new scopt.OptionParser[Params]("spark-gm-ingest") {
    head("spark-gm-ingest", "0.1")

    arg[WaveOrMesa]("'geowave' or 'geomesa'...")
      .action( (s, conf) => conf.copy(waveOrMesa = s) )
      .text("geowave or geomesa [default=geomesa]")
    opt[FileFormat]('f', "format")
      .action( (s, conf) => conf.copy(fileFormat = s) )
      .text("The file format to ingest (shp or csv)")
      .required
    opt[String]('i',"instance")
      .action( (s, conf) => conf.copy(instanceId = s) )
      .text("Accumulo instance ID [default=geomesa]")
      .required
    opt[String]('z',"zookeepers")
      .action( (s, conf) => conf.copy(zookeepers = s) )
      .text("Zookeepers [default=zookeeper]")
      .required
    opt[String]('u',"user")
      .action( (s, conf) => conf.copy(user = s) )
      .text("User namer [default=root]")
      .required
    opt[String]('p',"password")
      .action( (s, conf) => conf.copy(password = s) )
      .text("Password [default=GisPwd]")
      .required
    opt[String]('t',"table")
      .action( (s, conf) => conf.copy(tableName = s) )
      .text("Table name")
      .required
    opt[String]('b', "bucket")
      .action( (s, conf) => conf.copy(s3bucket = s) )
      .text("S3 Bucket containing shapefiles to ingest")
      .required
    opt[String]('d', "directory")
      .action( (s, conf) => conf.copy(s3prefix = s) )
      .text("S3 directory (prefix) containing shapefiles to ingest")
      .required
    help("help").text("Print this usage text")
  }
}
