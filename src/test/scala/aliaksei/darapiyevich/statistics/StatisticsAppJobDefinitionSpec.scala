package aliaksei.darapiyevich.statistics

import aliaksei.darapiyevich.{InputDefinition, JobDefinition, OutputDefinition, UnitTestSpec}

class StatisticsAppJobDefinitionSpec extends UnitTestSpec{
  private val OutputFormat = "outputFormat"
  private val OutputBasePath = "/output/base/path"

  private val Subfolder = "child"
  private val ExpectedOutputDefinition = OutputDefinition(
    OutputFormat,
    s"$OutputBasePath/$Subfolder"
  )

  private val given = JobDefinition(
    InputDefinition("inputFormat", "/input/path"),
    OutputDefinition(OutputFormat, OutputBasePath)
  )

  import StatisticsAppJobDefinition._

  "withOutputSubfolder" should "not modify input definition" in {
    val result = given.withOutputSubfolder(Subfolder)
    result.inputDefinition shouldEqual given.inputDefinition
  }

  it should "add subfolder name to output path" in {
    val result = given.withOutputSubfolder(Subfolder)
    result.outputDefinition shouldEqual ExpectedOutputDefinition
  }
}
