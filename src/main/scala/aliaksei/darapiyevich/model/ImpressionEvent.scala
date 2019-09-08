package aliaksei.darapiyevich.model

import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object ImpressionEvent {
  val columns = new {
    val Category: String = "category"
    val Product: String = "product"
    val UserId: String = "userId"
    val EventTime: String = "eventTime"
    val EventType: String = "eventType"
  }

  import columns._

  val schema: StructType = StructType(
    Seq(
      StructField(Category, StringType),
      StructField(Product, StringType),
      StructField(UserId, StringType),
      StructField(EventTime, TimestampType),
      StructField(EventType, StringType)
    )
  )

}
