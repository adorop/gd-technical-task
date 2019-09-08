package aliaksei.darapiyevich.impression.enrichment

import aliaksei.darapiyevich.SparkTransformationsSpec
import aliaksei.darapiyevich.model.{ImpressionEvent, Session}
import org.apache.spark.sql.types.StructType

class EnrichEventWithSessionInfoTransformationSpec extends SparkTransformationsSpec {
  private val FiveMinutes = 5 * 60

  private val transform = new EnrichEventWithSessionInfoTransformation(sessionInactivityThresholdSeconds = FiveMinutes)

  override def inputSchema: StructType = ImpressionEvent.schema

  override def expectedSchema: StructType = {
    import aliaksei.darapiyevich.utils.SparkUtils._
    ImpressionEvent.schema + Session.schema
  }

  test("enriches impression event with session info") {
    val sortColumn = ImpressionEvent.columns.EventTime
    val result = transform(input)
      .orderBy(sortColumn)
      .collect()
    result should equal (expected.orderBy(sortColumn).collect())
  }

}
