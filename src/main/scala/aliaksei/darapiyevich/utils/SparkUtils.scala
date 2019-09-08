package aliaksei.darapiyevich.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.col

object SparkUtils {
  private val DefaultSparkMaster = "local[*]"

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
}
