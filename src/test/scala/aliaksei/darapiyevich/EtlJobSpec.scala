package aliaksei.darapiyevich

import aliaksei.darapiyevich.EtlJob.LoadFactory
import org.apache.spark.sql.{DataFrame, Row}
import org.mockito.Mockito._

class EtlJobSpec extends UnitTestSpec {

  trait Setup {
    val inputDefinition: InputDefinition = InputDefinition("inputFormat", "inputLocation")
    val outputDefinition: OutputDefinition = mock[OutputDefinition]
    val jobDefinition: JobDefinition = JobDefinition(inputDefinition, outputDefinition)

    val input: DataFrame = mock[DataFrame]
    val transformedInput: DataFrame = mock[DataFrame]

    val extract: Extract[Row] = setupExtract()
    val transform: Transform[Row, Row] = setupTransform()
    val load: Load[Row] = mock[Load[Row]]
    val loadFactory: LoadFactory[Row] = setupLoad()

    val job: EtlJob[Row, Row] = new EtlJob(extract, transform, loadFactory)

    def setupExtract(): Extract[Row] = {
      val extract = mock[Extract[Row]]
      when(extract.from(inputDefinition)).thenReturn(input)
      extract
    }

    def setupTransform(): Transform[Row, Row] = {
      val transform = mock[Transform[Row, Row]]
      when(transform.apply(input)).thenReturn(transformedInput)
      transform
    }

    def setupLoad(): LoadFactory[Row] = {
      val factory = mock[LoadFactory[Row]]
      when(factory.apply(transformedInput)).thenReturn(load)
      factory
    }
  }

  "EtlJob" should "extract-transform-load from/to defined locations" in new Setup {
    job.run(jobDefinition)
    verify(load).to(outputDefinition)
  }

}
