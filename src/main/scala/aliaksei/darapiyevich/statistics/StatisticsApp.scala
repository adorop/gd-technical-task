package aliaksei.darapiyevich.statistics

import aliaksei.darapiyevich.impression.enrichment.{EnrichEventWithSessionInfoTransformation, ImpressionEventAppArgsParser, ImpressionEventExtractor}
import aliaksei.darapiyevich.model.ImpressionEvent
import aliaksei.darapiyevich.utils.SparkUtils
import aliaksei.darapiyevich.{DataFrameLoader, EtlJob, JobDefinition}
import org.apache.spark.sql.{Row, SparkSession}

object StatisticsApp extends App {

  val argsParser = new ImpressionEventAppArgsParser(args)
  val jobDefinition = JobDefinition.fromArgs(argsParser, inputSchema = Some(ImpressionEvent.schema))

  val spark = SparkSession.builder()
    .appName("StatisticsApp")
    .master(SparkUtils.sparkMaster)
    .getOrCreate()

  new EtlJob[Row, Row](
    new ImpressionEventWithSessionInfoExtractor(
      new ImpressionEventExtractor(spark),
      new EnrichEventWithSessionInfoTransformation(argsParser.sessionExpirationThresholdSeconds)
    ),
    new CategoryMedianSessionDurationTransformation(),
    DataFrameLoader.factory
  ).run(jobDefinition)
}
