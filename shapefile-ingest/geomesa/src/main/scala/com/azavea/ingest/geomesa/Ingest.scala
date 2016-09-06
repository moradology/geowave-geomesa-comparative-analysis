package com.azavea.ingest.geomesa

import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.simple.SimpleFeatureStore
import org.opengis.feature.simple._
import org.geotools.feature.simple._
import org.opengis.feature.`type`.Name
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.data.{DataStoreFinder, DataUtilities, FeatureWriter, Transaction}

import java.util.HashMap
import scala.collection.JavaConversions._

import com.azavea.ingest.common._

object Ingest {
  case class Params (csvOrShp: String = "",
                     instanceId: String = "geomesa",
                     zookeepers: String = "zookeeper",
                     user: String = "root",
                     password: String = "GisPwd",
                     tableName: String = "",
                     dropLines: Int = 0,
                     separator: String = "\t",
                     codec: CSVSchemaParser.Expr = CSVSchemaParser.Spec(Nil),
                     featureName: String = "",
                     s3bucket: String = "",
                     s3prefix: String = "",
                     csvExtension: String = ".csv",
                     unifySFT: Boolean = true) {

    def convertToJMap(): HashMap[String, String] = {
      val result = new HashMap[String, String]
      result.put("instanceId", instanceId)
      result.put("zookeepers", zookeepers)
      result.put("user", user)
      result.put("password", password)
      result.put("tableName", tableName)
      result
    }
  }

  def registerSFTs(cli: Params)(rdd: RDD[SimpleFeature]) =
    rdd.foreachPartition({ featureIter =>
      val ds = DataStoreFinder.getDataStore(cli.convertToJMap)

      if (ds == null) {
        println("Could not build AccumuloDataStore")
        java.lang.System.exit(-1)
      }

      featureIter.toStream.map({feature =>
        feature.getFeatureType
      }).distinct.foreach({ sft =>
        ds.createSchema(sft)
        ds.dispose
      })
    })

  def registerSFT(cli: Params)(sft: SimpleFeatureType) = {
    val ds = DataStoreFinder.getDataStore(cli.convertToJMap)

    if (ds == null) {
      println("Could not build AccumuloDataStore")
      java.lang.System.exit(-1)
    }

    ds.createSchema(sft)
    ds.dispose
  }

  def ingestRDD(cli: Params)(rdd: RDD[SimpleFeature]) =
    rdd.foreachPartition({ featureIter =>
      val ds = DataStoreFinder.getDataStore(cli.convertToJMap)

      if (ds == null) {
        println("Could not build AccumuloDataStore")
        java.lang.System.exit(-1)
      }

      // The method for ingest here is based on:
      // https://github.com/locationtech/geomesa/blob/master/geomesa-tools/src/main/scala/org/locationtech/geomesa/tools/accumulo/ingest/AbstractIngest.scala#L104
      featureIter.toStream.groupBy(_.getName.toString).foreach({ case (typeName: String, features: SimpleFeature) =>
        val fw = ds.getFeatureWriterAppend(typeName, Transaction.AUTO_COMMIT)
        features.foreach({ feature =>
          val toWrite = fw.next()
          toWrite.setAttributes(feature.getAttributes)
          toWrite.getIdentifier.asInstanceOf[FeatureIdImpl].setID(feature.getID)
          toWrite.getUserData.putAll(feature.getUserData)
          toWrite.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
          try {
            fw.write()
          } catch {
            case e: Exception =>
              println(s"Failed to write a feature", e)
          } finally {
            fw.close()
          }
        })
      })

      ds.dispose
    })

  def registerAndIngestRDD(cli: Params)(rdd: RDD[SimpleFeature]) = {
    registerSFTs(cli)(rdd)
    ingestRDD(cli)(rdd)
  }
}