package aliaksei.darapiyevich

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.mockito.Mockito._

class CachingDataFrameExtractorSpec extends UnitTestSpec {

  trait Setup {
    val inputDefinition = InputDefinition("format", "/path")

    val inputDataFrame: DataFrame = setupInputDataFrame()

    private def setupInputDataFrame(): DataFrame = {
      val inputDf = mock[DataFrame]
      when(inputDf.cache()).thenReturn(inputDf)
      when(inputDf.select()).thenReturn(inputDf)
      inputDf
    }

    class MockExtract extends Extract[Row] {
      override def from(inputDefinition: InputDefinition): DataFrame = inputDataFrame.select()
    }

  }

  "Caching extractor" should "return cached DataFrame" in new Setup {
    val cachingExtract = new MockExtract with CachingDataFrameExtractor
    cachingExtract from inputDefinition
    verify(inputDataFrame).cache()
  }

  it should "not run extract and cache second time" in new Setup {
    val cachingExtract = new MockExtract with CachingDataFrameExtractor
    cachingExtract from inputDefinition
    cachingExtract from inputDefinition
    verify(inputDataFrame, times(1)).select()
    verify(inputDataFrame, times(1)).cache()
  }

}
