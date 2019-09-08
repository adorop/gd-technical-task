package aliaksei.darapiyevich.impression.enrichment

import aliaksei.darapiyevich.SparkTransformationsSpec
import aliaksei.darapiyevich.model.{ImpressionEvent, Session}
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.col

class EnrichEventWithSessionInfoTransformationSpec extends SparkTransformationsSpec {
  private val FiveMinutes = 5 * 60

  override def inputSchema: StructType = ImpressionEvent.schema

  override def expectedSchema: StructType = {
    import aliaksei.darapiyevich.utils.SparkUtils._
    ImpressionEvent.schema + Session.schema
  }

  override def sortColumn: Column = col(ImpressionEvent.columns.EventTime)

  test("enriches impression event with session info") {
    test(new EnrichEventWithSessionInfoTransformation(sessionInactivityThresholdSeconds = FiveMinutes))
  }

}
