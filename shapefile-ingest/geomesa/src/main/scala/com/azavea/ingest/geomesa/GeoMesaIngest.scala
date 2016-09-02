package com.azavea.ingest.geomesa

import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.simple.SimpleFeatureStore
import org.opengis.feature.simple._
import org.opengis.feature.`type`.Name
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.utils.geotools.GeneralShapefileIngest
import org.geotools.data.{DataStoreFinder, DataUtilities, FeatureWriter, Transaction}


import java.util.HashMap
import scala.collection.JavaConversions._

import com.azavea.ingest.common._

object GeoMesaIngest {

  def registerSFTs(params: CommandLine.Params, rdd: RDD[SimpleFeature])(implicit sc: SparkContext): Unit =
    rdd.foreachPartition({ featureIter =>
      val ds = DataStoreFinder.getDataStore(params.convertToJMap)

      if (ds == null) {
        println("Could not build AccumuloDataStore")
        java.lang.System.exit(-1)
      }

      val uniqueSFTs = featureIter.toStream.map({feature =>
        println(s"Feature encountered: ${feature.getName.toString}")
        feature.getFeatureType
      }).distinct

      uniqueSFTs.foreach({ sft =>
        ds.createSchema(sft)
        ds.dispose
      })
    })

  def ingestRDD(params: CommandLine.Params, rdd: RDD[SimpleFeature])(implicit sc: SparkContext): Unit =
    rdd.foreachPartition({ featureIter =>
      val ds = DataStoreFinder.getDataStore(params.convertToJMap)

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

  def registerAndIngestRDD(params: CommandLine.Params, rdd: RDD[SimpleFeature])(implicit sc: SparkContext): Unit = {
    registerSFTs(params, rdd)
    ingestRDD(params, rdd)
  }
}
