package aliaksei.darapiyevich.statistics

import aliaksei.darapiyevich.{Extract, InputDefinition, Transform}
import org.apache.spark.sql.{Dataset, Row}

class ImpressionEventWithSessionInfoExtractor(
                                               extractImpressionEvents: Extract[Row],
                                               enrichWithSessionInfo: Transform[Row, Row]
                                             ) extends Extract[Row]{
  override def from(inputDefinition: InputDefinition): Dataset[Row] = {
    val impressionEvents = extractImpressionEvents from inputDefinition
    enrichWithSessionInfo(impressionEvents)
  }
}
