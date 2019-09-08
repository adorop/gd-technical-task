package aliaksei.darapiyevich.statistics

import aliaksei.darapiyevich.Transform
import aliaksei.darapiyevich.utils.SparkUtils.{patchNull, timeDiffSeconds}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, Row}

class NumberOfUsersByTimeSpentPerCategoryTransformation extends Transform[Row, Row] {

  import NumberOfUsersByTimeSpentPerCategoryTransformation._
  import aliaksei.darapiyevich.model.ImpressionEvent.columns._

  override def apply(eventWithSessionInfo: Dataset[Row]): Dataset[Row] = {
    eventWithSessionInfo.withColumn(
      TimeSpentIntervalRange.FieldName,
      TimeSpentIntervalRange.Expression
    )
      .groupBy(col(Category))
      .pivot(
        pivotColumn = TimeSpentIntervalRange.FieldName,
        values = Seq(TimeSpentIntervalRange.LtOne, TimeSpentIntervalRange.OneToFive, TimeSpentIntervalRange.GtFive))
      .agg(countDistinct(col(UserId)))
      .select(
        col(Category),
        patchNull(TimeSpentIntervalRange.LtOne, 0),
        patchNull(TimeSpentIntervalRange.OneToFive, 0),
        patchNull(TimeSpentIntervalRange.GtFive, 0)
      )
  }
}

object NumberOfUsersByTimeSpentPerCategoryTransformation {

  import aliaksei.darapiyevich.model.Session.columns._

  private val OneMinute = 60
  private val FiveMinutes = OneMinute * 5

  val TimeSpentIntervalRange = new {
    val LtOne = "lt_one_minute"
    val OneToFive = "one_to_five_minutes"
    val GtFive = "gt_five_minutes"

    val FieldName = "time_spent_range"

    val Expression: Column = {
      val sessionDuration = timeDiffSeconds(col(SessionEndTime), col(SessionStartTime))
      when(sessionDuration < OneMinute, lit(LtOne))
        .when(sessionDuration > FiveMinutes, lit(GtFive))
        .otherwise(OneToFive)
    }
  }
}
