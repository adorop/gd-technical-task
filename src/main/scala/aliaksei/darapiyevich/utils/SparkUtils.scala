package aliaksei.darapiyevich.utils

import java.math.{BigDecimal, RoundingMode}

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object SparkUtils {
  private val DefaultSparkMaster = "local[*]"
  private val DecimalScale = 2
  private val DecimalRoundMode = RoundingMode.HALF_UP

  implicit class ReachSchema(structType: StructType) {
    def +(other: StructType): StructType = {
      StructType(structType.fields ++ other.fields)
    }
  }

  implicit def fieldNames2columns(fieldsNames: Array[String]): Array[Column] = {
    fieldsNames.map(col)
  }

  def sparkMaster: String = {
    val maybeSparkMaster = Option(System.getProperty("spark.master"))
    maybeSparkMaster.getOrElse(DefaultSparkMaster)
  }

  def medianUDF: UserDefinedFunction = {
    udf((values: Seq[BigDecimal]) => median(values))
  }

  private[utils] def median(values: Seq[BigDecimal]): Double = {
    val sorted = values.sorted
    val size = values.size
    if (size % 2 == 0) {
      getMeanOfTwoInMiddle(sorted)
    } else {
      sorted(size / 2)
        .setScale(DecimalScale, DecimalRoundMode)
        .doubleValue()
    }
  }

  private def getMeanOfTwoInMiddle(sorted: Seq[BigDecimal]): Double = {
    val firstInMiddle = sorted.size / 2 - 1
    val secondInMiddle = sorted.size / 2
    sorted(firstInMiddle).add(sorted(secondInMiddle))
      .divide(new BigDecimal("2"), DecimalScale, DecimalRoundMode)
      .doubleValue()
  }

  def timeDiffSeconds(followingTimestamp: Column, precedingTimestamp: Column): Column = {
    unix_timestamp(followingTimestamp) - unix_timestamp(precedingTimestamp)
  }

  def patchNull(column: String, default: Any): Column = {
    when(not(isnull(col(column))), col(column))
      .otherwise(default) as column
  }
}
