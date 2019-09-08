package aliaksei.darapiyevich

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.scalatest.{FunSuite, Matchers}

trait SparkTransformationsSpec extends FunSuite with Matchers {

  val spark: SparkSession = SparkSession.builder()
    .appName(getClass.getSimpleName)
    .master("local[*]")
    .config("spark.ui.enabled", false)
    .getOrCreate()

  def inputSchema: StructType

  def expectedSchema: StructType

  def input: DataFrame = {
    spark.read
      .schema(inputSchema)
      .option("header", true)
      .csv(pathFromResource(fixtureCsvResource))
  }

  def pathFromResource(resourcePath: String): String = {
    getClass.getResource(resourcePath).getPath
  }

  def fixtureCsvResource: String = s"/fixtures/${getClass.getSimpleName}.csv"

  def expected: DataFrame = {
    spark.read
      .schema(expectedSchema)
      .option("header", true)
      .csv(pathFromResource(expectedCsvResource))
  }

  def expectedCsvResource: String = s"/expected/${getClass.getSimpleName}.csv"

  def test(transformation: Transform[Row, Row]): Unit = {
    val transformed = transformation(input)
    val result = transformed.orderBy(sortColumn)
      .collect()
    result should equal(expected.orderBy(sortColumn).collect())
  }

  def sortColumn: Column
}
