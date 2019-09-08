package aliaksei.darapiyevich.impression.enrichment

import aliaksei.darapiyevich.Transform
import aliaksei.darapiyevich.model.{ImpressionEvent, Session}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, Dataset, Row}

class EnrichEventWithSessionInfoTransformation(
                                                sessionInactivityThresholdSeconds: Int
                                              ) extends Transform[Row, Row] {

  override def apply(input: Dataset[Row]): Dataset[Row] = {
    import EnrichEventWithSessionInfoTransformation._
    import aliaksei.darapiyevich.model.ImpressionEvent.columns.EventTime
    import aliaksei.darapiyevich.model.Session.columns._
    import aliaksei.darapiyevich.utils.SparkUtils._

    val outputProjection: Array[Column] = (ImpressionEvent.schema + Session.schema).fieldNames
    input.toDF()
      .withColumn(NotActiveSeconds.fieldName, NotActiveSeconds.expression)
      .withColumn(IsStartOfNewSession.fieldName, IsStartOfNewSession.expression(sessionInactivityThresholdSeconds))
      .withColumn(LocalSessionId.fieldName, LocalSessionId.expression)
      .withColumn(SessionId, SessionIdExpression)
      .withColumn(SessionStartTime, min(EventTime) over sessionWindow)
      .withColumn(SessionEndTime, max(EventTime) over sessionWindow)
      .select(outputProjection: _*)
  }
}

object EnrichEventWithSessionInfoTransformation {

  import aliaksei.darapiyevich.model.ImpressionEvent.columns._

  private val LocalSessionIdName = "local_session_id"

  private val userIdAndCategoryWindow = Window.partitionBy(UserId, Category)
    .orderBy(col(EventTime))

  private val sessionWindow = Window.partitionBy(UserId, Category, LocalSessionIdName)

  private val NotActiveSeconds = new {
    val fieldName = "not_active_seconds"

    val expression: Column = {
      val current = col(EventTime)
      val previous = lag(EventTime, 1) over userIdAndCategoryWindow
      unix_timestamp(current) - unix_timestamp(previous)
    }
  }

  private val IsStartOfNewSession = new {
    val fieldName = "is_start_of_new_session"

    def expression(sessionInactivityThresholdSeconds: Int): Column = {
      val firstUserIdCategoryOccurrence = col(NotActiveSeconds.fieldName).isNull
      val sessionExpired = col(NotActiveSeconds.fieldName) > lit(sessionInactivityThresholdSeconds)
      (firstUserIdCategoryOccurrence or sessionExpired) cast IntegerType
    }
  }

  private val LocalSessionId = new {
    val fieldName = LocalSessionIdName

    val expression: Column = sum(IsStartOfNewSession.fieldName) over userIdAndCategoryWindow
  }

  private val SessionIdExpression: Column = concat(
    removeSpaces(col(Category)),
    lit("_"),
    removeSpaces(col(UserId)),
    lit("_"),
    col(LocalSessionIdName)
  )

  private def removeSpaces(column: Column): Column = {
    regexp_replace(column, " ", "")
  }
}
