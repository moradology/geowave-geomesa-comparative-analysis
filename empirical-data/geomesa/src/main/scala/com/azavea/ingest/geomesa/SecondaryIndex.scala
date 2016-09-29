package com.azavea.ingest.geomesa

import org.apache.spark._
import org.apache.spark.rdd._
import org.opengis.feature.simple._
import org.geotools.feature.simple._

import scala.collection.JavaConverters._

import com.azavea.ingest.common._

object SecondaryIndexing {
  def addIndices(rdd: RDD[SimpleFeature], indices: Map[String, SecondaryIndex]): RDD[SimpleFeature] =
    rdd.mapPartitions({ featureIter =>
      val bufferedFeatures = featureIter.buffered
      val headFeature = bufferedFeatures.head

      val builder = new SimpleFeatureTypeBuilder
      builder.setName(headFeature.getType.getName)
      builder.addAll(headFeature.getType.getAttributeDescriptors)
      headFeature.getUserData.asScala.map({ case (key, value) => builder.userData(key, value) })

      val sft = builder.buildFeatureType()

      indices.foreach({ case (fieldName, index) =>
        println(s"fieldname is $fieldName")
        index.kind match {
          case JoinIndex =>
            sft.getDescriptor(fieldName).getUserData.put("index", "join")
          case FullIndex =>
            sft.getDescriptor(fieldName).getUserData.put("index", "full")
        }
        index.cardinality match {
          case HighCardinality =>
            sft.getDescriptor(fieldName).getUserData.put("cardinality", "high")
          case LowCardinality =>
            sft.getDescriptor(fieldName).getUserData.put("cardinality", "low")
        }
      })

      bufferedFeatures.map({ feature =>
        SimpleFeatureBuilder.retype(feature, sft)
      })
    })
}
