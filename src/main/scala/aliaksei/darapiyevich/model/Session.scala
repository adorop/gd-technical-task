package aliaksei.darapiyevich.model

import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object Session {
  val columns = new {
    val SessionId: String = "sessionId"
    val SessionStartTime: String = "sessionStartTime"
    val SessionEndTime: String = "sessionEndTime"
  }

  import columns._

  val schema = StructType(
    Seq(
      StructField(SessionId, StringType),
      StructField(SessionStartTime, TimestampType),
      StructField(SessionEndTime, TimestampType)
    )
  )
}
