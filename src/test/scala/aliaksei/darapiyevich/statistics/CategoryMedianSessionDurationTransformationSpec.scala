package aliaksei.darapiyevich.statistics

import aliaksei.darapiyevich.SparkTransformationsSpec
import aliaksei.darapiyevich.model.{ImpressionEvent, Session}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class CategoryMedianSessionDurationTransformationSpec extends SparkTransformationsSpec {
  override def inputSchema: StructType = {
    import aliaksei.darapiyevich.utils.SparkUtils._

    ImpressionEvent.schema + Session.schema
  }

  import CategoryMedianSessionDurationTransformation._
  import ImpressionEvent.columns._

  override def expectedSchema: StructType = {
    StructType(
      Seq(
        StructField(Category, StringType),
        StructField(MedianSessionDurationFieldName, DoubleType)
      )
    )
  }

  test("calculates median session duration for each category") {
    test(new CategoryMedianSessionDurationTransformation)
  }

  test("calculate median session duration for each category without approximation") {
    test(new CategoryMedianSessionDurationTransformation(canTolerateApproximation = false))
  }

  override def sortColumn: Column = col(Category)
}
