package aliaksei.darapiyevich.impression.enrichment

import aliaksei.darapiyevich.{InputDefinition, JobDefinition, OutputDefinition, UnitTestSpec}

class ImpressionEventAppArgsParserSpec extends UnitTestSpec {
  private val InputFormat = "csv"
  private val InputLocation = "/input/path"
  private val OutputFormat = "parquet"
  private val OutputLocation = "/output/path"

  private val args = Array(
    "--input-format", InputFormat,
    "--input-location", InputLocation,
    "--output-format", OutputFormat,
    "--output-location", OutputLocation
  )

  private val expectedJobDefinition = JobDefinition(
    InputDefinition(
      InputFormat,
      InputLocation
    ),
    OutputDefinition(
      OutputFormat,
      OutputLocation
    )
  )

  "Parser" should "contain job definition" in {
    val parser = new ImpressionEventAppArgsParser(args)
    val jobDefinition = JobDefinition.fromArgs(parser)
    jobDefinition should equal (expectedJobDefinition)
  }

  it should "contain default session expiration threshold when not given" in {
    val parser = new ImpressionEventAppArgsParser(args)
    parser.sessionExpirationThresholdSeconds shouldBe ImpressionEventAppArgsParser.DefaultSessionExpirationThresholdSeconds
  }

}
