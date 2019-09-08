package aliaksei.darapiyevich.impression.enrichment

import aliaksei.darapiyevich.{Extract, InputDefinition}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class ImpressionEventExtractor(spark: SparkSession) extends Extract[Row] {
  override def from(inputDefinition: InputDefinition): Dataset[Row] = {
    val dataFrameReader = spark.read
      .format(inputDefinition.format)

    inputDefinition.schema
      .map(dataFrameReader.schema)
      .getOrElse(dataFrameReader)
      .load(inputDefinition.location)
  }
}
