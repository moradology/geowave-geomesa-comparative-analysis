package com.azavea.ca.server

import com.azavea.ca.core._
import com.azavea.ca.server.results._
import com.azavea.ca.server.geomesa.connection.GeoMesaConnection
import com.azavea.ca.server.geowave.connection.GeoWaveConnection
import com.azavea.ca.server.geowave.GeoWaveQuerier

import akka.http.scaladsl.server.Directives._
import de.heikoseeberger.akkahttpcirce._
import io.circe.generic.auto._
import geotrellis.vector._
import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL
import org.opengis.filter.Filter

import scala.concurrent.Future

object SecondaryAttributeIndices
    extends BaseService
    with CAQueryUtils
    with CirceSupport
    with AkkaSystem.LoggerExecutor {

  val gwTableName = "geowave.gdelt"
  val gwFeatureTypeName = "gdelt-event"

  val gmTableName = "geomesa.gdelt"
  val gmFeatureTypeName = "gdelt-event"

  def routes =
    pathPrefix("gdelt") {
      pathPrefix("pringles") {
        pathEndOrSingleSlash {
          get {
            complete { Future { "pong" } } }
        }
      } ~
      pathPrefix("reset") {
        pathEndOrSingleSlash {
          get {
            complete { Future { resetDataStores() ; "done" } } }
        }
      } ~
      pathPrefix("secondary-index") {
        pathPrefix("french-actor-code-temporal") {
          val queryName = "GDELT-AUX-INDEX-1-YEAR"

          pathEndOrSingleSlash {
            get {
              parameters('test ?, 'loose ?, 'wOrm ? "both") { (isTestOpt, isLooseOpt, waveOrMesa) =>
                val isTest = checkIfIsTest(isTestOpt)
                complete {
                  Future {
                    val tq = TimeQuery("2001-01-01T00:00:00", "2001-02-07T00:00:00")

                    val gwIdxQuery = ECQL.toFilter(tq.toCQL("day") + " AND " + "actor1_code_idx = 'FRA'")
                    val gwNGramIdxQuery = ECQL.toFilter(tq.toCQL("day") + " AND " + "actor1_code_idx LIKE 'FR%'")
                    val gwNoIdxQuery = ECQL.toFilter(tq.toCQL("day") + " AND " + "actor1_code = 'FRA'")

                    //val gmNoIdxQuery = ECQL.toFilter(tq.toCQL("day") + " AND " + "actor1_code = 'FRA'")
                    //val gmIdxQueryFLC = ECQL.toFilter(tq.toCQL("day") + " AND " + "actor1_code_flc = 'FRA'")
                    //val gmIdxQueryFHC = ECQL.toFilter(tq.toCQL("day") + " AND " + "actor1_code_fhc = 'FRA'")
                    //val gmIdxQueryJLC = ECQL.toFilter(tq.toCQL("day") + " AND " + "actor1_code_jlc = 'FRA'")
                    //val gmIdxQueryJHC = ECQL.toFilter(tq.toCQL("day") + " AND " + "actor1_code_jhc = 'FRA'")

                    val gwNoIndexQuery = captureGeoWaveQuery(gwNoIdxQuery)
                    val gwIndexQuery = captureGeoWaveQuery(gwIdxQuery)
                    val gwNGramQuery = captureGeoWaveQuery(gwNGramIdxQuery)

                    //val gmNoIndexQuery = captureGeoMesaQuery(gmNoIdxQuery, checkIfIsLoose(isLooseOpt))
                    //val gmFullLowCardinalityIndexQuery = captureGeoMesaQuery(gmIdxQueryFLC, checkIfIsLoose(isLooseOpt))
                    //val gmFullHighCardinalityIndexQuery = captureGeoMesaQuery(gmIdxQueryFHC, checkIfIsLoose(isLooseOpt))
                    //val gmJoinLowCardinalityIndexQuery = captureGeoMesaQuery(gmIdxQueryJLC, checkIfIsLoose(isLooseOpt))
                    //val gmJoinHighCardinalityIndexQuery = captureGeoMesaQuery(gmIdxQueryJHC, checkIfIsLoose(isLooseOpt))

                    val result = Seq(
                      RunResult(s"${queryName}NoIdx${looseSuffix(isLooseOpt)}", None, Some(gwNoIndexQuery), isTest),
                      RunResult(s"${queryName}Idx${looseSuffix(isLooseOpt)}", None, Some(gwIndexQuery), isTest),
                      RunResult(s"${queryName}NgramIdx${looseSuffix(isLooseOpt)}", None, Some(gwNGramQuery), isTest)
                      //RunResult(s"${queryName}NoIdx${looseSuffix(isLooseOpt)}", Some(gmNoIndexQuery), None, isTest),
                      //RunResult(s"${queryName}FLC${looseSuffix(isLooseOp t)}", Some(gmFullLowCardinalityIndexQuery), None, isTest),
                      //RunResult(s"${queryName}FHC${looseSuffix(isLooseOpt)}", Some(gmFullHighCardinalityIndexQuery), None, isTest),
                      //RunResult(s"${queryName}JLC${looseSuffix(isLooseOpt)}", Some(gmJoinLowCardinalityIndexQuery), None, isTest),
                      //RunResult(s"${queryName}JHC${looseSuffix(isLooseOpt)}", Some(gmJoinHighCardinalityIndexQuery), None, isTest)
                    )
                    result.foreach(DynamoDB.saveResult(_))
                    result
                  }
                }
              }
            }
          }
        } ~
        pathPrefix("french-actor-code-spatial") {
          val queryName = "GDELT-AUX-INDEX-BBOX-IN-FRANCE"

          pathEndOrSingleSlash {
            get {
              parameters('test ?, 'loose ?, 'wOrm ? "both") { (isTestOpt, isLooseOpt, waveOrMesa) =>
                val isTest = checkIfIsTest(isTestOpt)
                complete {
                  Future {
                    val tq = TimeQuery("2001-01-01T00:00:00", "2001-01-07T00:00:00")

                    val gwIdxQuery = ECQL.toFilter(CQLUtils.toBBOXquery("the_geom", France.regions.head.envelope) + " AND " + "actor1_code_idx = 'FRA'")
                    val gwNGramIdxQuery = ECQL.toFilter(CQLUtils.toBBOXquery("the_geom", France.regions.head.envelope) + " AND " + "actor1_code_idx LIKE 'FR%'")
                    val gwNoIdxQuery = ECQL.toFilter(CQLUtils.toBBOXquery("the_geom", France.regions.head.envelope) + " AND " + "actor1_code = 'FRA'")

                    //val gmNoIdxQuery = ECQL.toFilter(CQLUtils.toBBOXquery("the_geom", France.regions.head.envelope) + " AND " + "actor1_code = 'FRA'")
                    //val gmIdxQueryFLC = ECQL.toFilter(CQLUtils.toBBOXquery("the_geom", France.regions.head.envelope) + " AND " + "actor1_code_flc = 'FRA'")
                    //val gmIdxQueryFHC = ECQL.toFilter(CQLUtils.toBBOXquery("the_geom", France.regions.head.envelope) + " AND " + "actor1_code_fhc = 'FRA'")
                    //val gmIdxQueryJLC = ECQL.toFilter(CQLUtils.toBBOXquery("the_geom", France.regions.head.envelope) + " AND " + "actor1_code_jlc = 'FRA'")
                    //val gmIdxQueryJHC = ECQL.toFilter(CQLUtils.toBBOXquery("the_geom", France.regions.head.envelope) + " AND " + "actor1_code_jhc = 'FRA'")

                    val gwNoIndexQuery = captureGeoWaveQuery(gwNoIdxQuery)
                    val gwIndexQuery = captureGeoWaveQuery(gwIdxQuery)
                    val gwNGramQuery = captureGeoWaveQuery(gwNGramIdxQuery)

                    //val gmNoIndexQuery = captureGeoMesaQuery(gmNoIdxQuery, checkIfIsLoose(isLooseOpt))
                    //val gmFullLowCardinalityIndexQuery = captureGeoMesaQuery(gmIdxQueryFLC, checkIfIsLoose(isLooseOpt))
                    //val gmFullHighCardinalityIndexQuery = captureGeoMesaQuery(gmIdxQueryFHC, checkIfIsLoose(isLooseOpt))
                    //val gmJoinLowCardinalityIndexQuery = captureGeoMesaQuery(gmIdxQueryJLC, checkIfIsLoose(isLooseOpt))
                    //val gmJoinHighCardinalityIndexQuery = captureGeoMesaQuery(gmIdxQueryJHC, checkIfIsLoose(isLooseOpt))

                    val result = Seq(
                      RunResult(s"${queryName}NoIdx${looseSuffix(isLooseOpt)}", None, Some(gwNoIndexQuery), isTest),
                      RunResult(s"${queryName}Idx${looseSuffix(isLooseOpt)}", None, Some(gwIndexQuery), isTest),
                      RunResult(s"${queryName}NgramIdx${looseSuffix(isLooseOpt)}", None, Some(gwNGramQuery), isTest)
                      //RunResult(s"${queryName}NoIdx${looseSuffix(isLooseOpt)}", Some(gmNoIndexQuery), None, isTest),
                      //RunResult(s"${queryName}FLC${looseSuffix(isLooseOpt)}", Some(gmFullLowCardinalityIndexQuery), None, isTest),
                      //RunResult(s"${queryName}FHC${looseSuffix(isLooseOpt)}", Some(gmFullHighCardinalityIndexQuery), None, isTest),
                      //RunResult(s"${queryName}JLC${looseSuffix(isLooseOpt)}", Some(gmJoinLowCardinalityIndexQuery), None, isTest),
                      //RunResult(s"${queryName}JHC${looseSuffix(isLooseOpt)}", Some(gmJoinHighCardinalityIndexQuery), None, isTest)
                    )
                    result.foreach(DynamoDB.saveResult(_))
                    result
                  }
                }
              }
            }
          }
        }
      }
    }
}
