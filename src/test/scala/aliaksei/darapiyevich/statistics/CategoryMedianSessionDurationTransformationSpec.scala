package aliaksei.darapiyevich.statistics

import aliaksei.darapiyevich.SparkTransformationsSpec
import aliaksei.darapiyevich.model.{ImpressionEvent, Session}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class CategoryMedianSessionDurationTransformationSpec extends SparkTransformationsSpec {
  override def inputSchema: StructType = {
    import aliaksei.darapiyevich.utils.SparkUtils._

    ImpressionEvent.schema + Session.schema
  }

  import ImpressionEvent.columns._

  override def expectedSchema: StructType = {
    StructType(
      Seq(
        StructField(Category, StringType),
        StructField("median_session_duraion", DoubleType)
      )
    )
  }

  test("calculates median session duration for each category") {
    val transform = new CategoryMedianSessionDurationTransformation
    val result = transform(input)
      .orderBy(Category)
      .collect()
    result should equal(expected.orderBy(Category).collect())
  }
}
