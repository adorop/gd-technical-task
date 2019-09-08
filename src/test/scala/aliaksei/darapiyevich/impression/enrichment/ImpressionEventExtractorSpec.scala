package aliaksei.darapiyevich.impression.enrichment

import aliaksei.darapiyevich.{Extract, InputDefinition, UnitTestSpec}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SparkSession}
import org.mockito.Mockito._

class ImpressionEventExtractorSpec extends UnitTestSpec {

  trait Setup {
    val Format: String = "csv"
    val Location = "/path"

    val spark: SparkSession = mock[SparkSession]
    val expected: DataFrame = mock[DataFrame]

    val dataFrameReader: DataFrameReader = setupDataFrameReader()

    val extract: Extract[Row] = new ImpressionEventExtractor(spark)

    def setupDataFrameReader() : DataFrameReader = {
      val reader = mock[DataFrameReader]
      when(spark.read).thenReturn(reader)
      when(reader.format(Format)).thenReturn(reader)
      when(reader.load(Location)).thenReturn(expected)
      reader
    }
  }

  trait SetupWithSchema extends Setup {
    val Schema: StructType = StructType(Seq(StructField("test_field", IntegerType)))

    when(dataFrameReader.schema(Schema)).thenReturn(mock[DataFrameReader])
  }

  "ImpressionEventExtractor" should "read DataFrame from specified location in given format" in new Setup {
    val inputDefinition = InputDefinition(Format, Location)
    val result = extract.from(inputDefinition)
    result shouldBe expected
  }

  it should "provide reader with schema when given" in new SetupWithSchema {
    val inputDefinitionWithSchema = InputDefinition(Format, Location, Some(Schema))
    extract.from(inputDefinitionWithSchema)
    verify(dataFrameReader).schema(Schema)
  }

}
