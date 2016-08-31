package com.azavea.ingest.core

import org.apache.spark.rdd.RDD
import org.opengis.feature.simple.SimpleFeature

import com.azavea.ingest.geomesa.GeoMesaIngest
import com.azavea.ingest.common.Shapefile

object Main {
  import com.azavea.ingest.common.CommandLine._
  def main(args: Array[String]): Unit = {
    val params: Params =
      parser.parse(args, Params()) match {
        case Some(params) => params
        case None => throw new IllegalArgumentException("Command line arguments failed to parse")
      }

    val featureRDD: RDD[SimpleFeature] = params.fileFormat match {
      case CSV => ???
      case SHP =>
        val urls = Shapefile.getShpUrls(params.s3bucket, params.s3prefix)
        Shapefile.shpUrls2shpRdd(urls)
    }

    params.waveOrMesa match {
      case GeoMesa =>
        GeoMesaIngest.registerAndIngestRDD(params)(featureRDD)
      case GeoWave => ???
    }
  }
}
