package aliaksei.darapiyevich.statistics

import aliaksei.darapiyevich.Transform
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.functions._

class CategoryMedianSessionDurationTransformation extends Transform[Row, Row] {
  override def apply(input: Dataset[Row]): Dataset[Row] = {
    import aliaksei.darapiyevich.model.ImpressionEvent.columns._
    import CategoryMedianSessionDurationTransformation._

    input
      .withColumn(SessionDuration.FieldName, SessionDuration.expression)
      .groupBy(Category)
      .agg(medianSessionDurationExpression as MedianSessionDurationFieldName)

  }
}

object CategoryMedianSessionDurationTransformation {

  import aliaksei.darapiyevich.model.Session.columns._

  val MedianSessionDurationFieldName = "median_session_duration_seconds"

  private val SessionDuration = new {
    val FieldName = "session_duration"
    val expression: Column = unix_timestamp(col(SessionEndTime)) - unix_timestamp(col(SessionStartTime))
  }

  private val medianSessionDurationExpression: Column = {
    expr(s"percentile_approx(${SessionDuration.FieldName}, 0.5)")
  }
}