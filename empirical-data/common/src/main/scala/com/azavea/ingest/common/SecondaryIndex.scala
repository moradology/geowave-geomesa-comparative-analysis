package com.azavea.ingest.common


sealed trait IndexCardinality
case object HighCardinality extends IndexCardinality
case object LowCardinality extends IndexCardinality

sealed trait IndexKind
case object JoinIndex extends IndexKind
case object FullIndex extends IndexKind

sealed trait SecondaryIndex {
  val cardinality: IndexCardinality
  val kind: IndexKind
}
case class NumericIdx(cardinality: IndexCardinality = LowCardinality, kind: IndexKind = JoinIndex) extends SecondaryIndex
case class TextIdx(cardinality: IndexCardinality = LowCardinality, kind: IndexKind = JoinIndex) extends SecondaryIndex
case class DateIdx(cardinality: IndexCardinality = LowCardinality, kind: IndexKind = JoinIndex) extends SecondaryIndex

