package aliaksei.darapiyevich.statistics

import aliaksei.darapiyevich.Transform
import aliaksei.darapiyevich.utils.SparkUtils.medianUDF
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, Row}

class CategoryMedianSessionDurationTransformation(
                                                   canTolerateApproximation: Boolean = true
                                                 ) extends Transform[Row, Row] {
  override def apply(input: Dataset[Row]): Dataset[Row] = {
    import CategoryMedianSessionDurationTransformation._
    import aliaksei.darapiyevich.model.ImpressionEvent.columns._

    val grouped = input
      .withColumn(SessionDuration.FieldName, SessionDuration.expression)
      .groupBy(Category)
    if (canTolerateApproximation) {
      grouped
        .agg(approximateMedianSessionDurationExpression as MedianSessionDurationFieldName)
    } else {
      grouped
        .agg(collect_list(col(SessionDuration.FieldName)) as "session_durations")
        .select(col(Category), medianUDF(col("session_durations")) as MedianSessionDurationFieldName)
    }
  }
}

object CategoryMedianSessionDurationTransformation {

  import aliaksei.darapiyevich.model.Session.columns._

  val MedianSessionDurationFieldName = "median_session_duration_seconds"

  private val SessionDuration = new {
    val FieldName = "session_duration"
    val expression: Column = unix_timestamp(col(SessionEndTime)) - unix_timestamp(col(SessionStartTime))
  }

  private val approximateMedianSessionDurationExpression: Column = {
    expr(s"percentile_approx(${SessionDuration.FieldName}, 0.5)")
  }
}