package com.azavea.ingest.geowave

import org.apache.spark._
import org.apache.spark.rdd._
import org.opengis.feature.simple._
import org.geotools.feature.simple._

import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfiguration
import mil.nga.giat.geowave.adapter.vector.utils.SimpleFeatureUserDataConfigurationSet
import mil.nga.giat.geowave.adapter.vector.index.TextSecondaryIndexConfiguration
import mil.nga.giat.geowave.adapter.vector.index.NumericSecondaryIndexConfiguration
import mil.nga.giat.geowave.adapter.vector.index.TemporalSecondaryIndexConfiguration

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.azavea.ingest.common._

object SecondaryIndexing {
  def addIndices(rdd: RDD[SimpleFeature], indices: Map[String, SecondaryIndex]): RDD[SimpleFeature] =
    rdd.mapPartitions({ featureIter =>
      val bufferedFeatures = featureIter.buffered
      val headFeature = bufferedFeatures.head

      val builder = new SimpleFeatureTypeBuilder
      builder.setName(headFeature.getType.getTypeName)
      builder.addAll(headFeature.getType.getAttributeDescriptors)
      headFeature.getUserData.asScala.map({ case (key, value) => builder.userData(key, value) })

      val sft = builder.buildFeatureType()

      val indicesByType: Map[String, Array[String]] = indices.map({ case (fieldName, idx) =>
        fieldName -> (idx match {
          case NumericIdx(_, _) => "numeric"
          case TextIdx(_, _) => "text"
          case DateIdx(_, _) => "temporal"
        })
      }).groupBy(_._2).mapValues({ lst =>
        lst.map(_._1).toArray
      }).toMap

      val secondaryIndexingConfigs = mutable.ArrayBuffer[SimpleFeatureUserDataConfiguration]()

      secondaryIndexingConfigs += new NumericSecondaryIndexConfiguration(
        indicesByType
          .getOrElse("numeric", Array[String]()).toArray.toSet.asJava
      )

      secondaryIndexingConfigs += new TextSecondaryIndexConfiguration(
        indicesByType.getOrElse("text", Array[String]()).toSet.asJava
      )

      secondaryIndexingConfigs += new TemporalSecondaryIndexConfiguration(
        indicesByType.getOrElse("temporal", Array[String]()).toSet.asJava
      )

      val config: SimpleFeatureUserDataConfigurationSet =
        new SimpleFeatureUserDataConfigurationSet(sft, secondaryIndexingConfigs.asJava)

      config.updateType(sft)

      bufferedFeatures.map({ feature =>
        SimpleFeatureBuilder.retype(feature, sft)
      })
    })
}
