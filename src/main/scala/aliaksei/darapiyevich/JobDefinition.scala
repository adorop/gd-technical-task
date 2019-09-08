package aliaksei.darapiyevich

import org.apache.spark.sql.types.StructType
import org.rogach.scallop.{ScallopConf, ScallopOption}

case class JobDefinition(
                          inputDefinition: InputDefinition,
                          outputDefinition: OutputDefinition
                        )

case class InputDefinition(
                            format: String,
                            location: String,
                            schema: Option[StructType] = None
                          )

case class OutputDefinition(
                             format: String,
                             location: String
                           )

object JobDefinition {
  def fromArgs(parser: BaseArgsParser, inputSchema: Option[StructType] = None): JobDefinition = {
    JobDefinition(
      InputDefinition(
        parser.inputFormat(),
        parser.inputLocation(),
        inputSchema
      ),
      OutputDefinition(
        parser.outputFormat(),
        parser.outputLocation()
      )
    )
  }
}

class BaseArgsParser(arguments: Seq[String]) extends ScallopConf(arguments) {
  lazy val inputFormat: ScallopOption[String] = opt[String](
    name = "input-format",
    descr = "Supported by Spark reader format",
    required = true,
    argName = "input-format",
    noshort = true
  )
  lazy val inputLocation: ScallopOption[String] = opt[String](
    name = "input-location",
    descr = "input URL",
    required = true,
    argName = "input-location",
    noshort = true
  )
  lazy val outputFormat: ScallopOption[String] = opt[String](
    name = "output-format",
    descr = "Supported by Spark writer format",
    required = true,
    argName = "output-format",
    noshort = true
  )
  lazy val outputLocation: ScallopOption[String] = opt[String](
    name = "output-location",
    descr = "Output URL",
    required = true,
    argName = "output-location",
    noshort = true
  )
  verify()
}