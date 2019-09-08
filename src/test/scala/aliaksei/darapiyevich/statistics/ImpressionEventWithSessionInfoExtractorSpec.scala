package aliaksei.darapiyevich.statistics

import aliaksei.darapiyevich.{Extract, InputDefinition, Transform, UnitTestSpec}
import org.apache.spark.sql.{DataFrame, Row}
import org.mockito.Mockito._

class ImpressionEventWithSessionInfoExtractorSpec extends UnitTestSpec {

  trait Setup {
    val inputDefinition = InputDefinition("format", "/path")

    val impressionEventDataFrame: DataFrame = mock[DataFrame]
    val enrichedDataFrame: DataFrame = mock[DataFrame]
    val impressionEventExtractor: Extract[Row] = setupImpressionEventExtractor()
    val enrichEventWithSessionInfoTransformation: Transform[Row, Row] = setupEnrichTransformation()

    val extract = new ImpressionEventWithSessionInfoExtractor(impressionEventExtractor, enrichEventWithSessionInfoTransformation)

    private def setupImpressionEventExtractor(): Extract[Row] = {
      val extractor = mock[Extract[Row]]
      when(extractor.from(inputDefinition)).thenReturn(impressionEventDataFrame)
      extractor
    }

    private def setupEnrichTransformation(): Transform[Row, Row] = {
      val transform = mock[Transform[Row, Row]]
      when(transform.apply(impressionEventDataFrame)).thenReturn(enrichedDataFrame)
      transform
    }
  }

  "Extractor" should "read impression events and enrich them with session statistics" in new Setup {
    val result = extract from inputDefinition
    result shouldBe enrichedDataFrame
  }

}
