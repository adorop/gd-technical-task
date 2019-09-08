package aliaksei.darapiyevich.statistics

import aliaksei.darapiyevich.SparkTransformationsSpec
import aliaksei.darapiyevich.model.{ImpressionEvent, Session}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class NumberOfUsersByTimeSpentPerCategoryTransformationSpec extends SparkTransformationsSpec{
  override def inputSchema: StructType = {
    import aliaksei.darapiyevich.utils.SparkUtils._

    ImpressionEvent.schema + Session.schema
  }

  import ImpressionEvent.columns.Category
  import NumberOfUsersByTimeSpentPerCategoryTransformation.TimeSpentIntervalRange

  override def expectedSchema: StructType = {

    StructType(
      Seq(
        StructField(Category, StringType),
        StructField(TimeSpentIntervalRange.LtOne, LongType),
        StructField(TimeSpentIntervalRange.OneToFive, LongType),
        StructField(TimeSpentIntervalRange.GtFive, LongType)
      )
    )
  }

  override def sortColumn: Column = col(Category)

  test("calculates number of unique users per category spending lt minute, 1-5 minutes, more than 5 minutes") {
    test(new NumberOfUsersByTimeSpentPerCategoryTransformation)
  }
}
