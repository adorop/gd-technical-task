package aliaksei.darapiyevich.impression.enrichment

import aliaksei.darapiyevich.model.ImpressionEvent
import aliaksei.darapiyevich.utils.SparkUtils
import aliaksei.darapiyevich.{DataFrameLoader, EtlJob, JobDefinition}
import org.apache.spark.sql.SparkSession

object ImpressionEventEnrichmentApp extends App {
  val argsParser = new ImpressionEventAppArgsParser(args)
  val jobDefinition = JobDefinition.fromArgs(argsParser, inputSchema = Some(ImpressionEvent.schema))
  val spark = SparkSession.builder()
    .appName("ImpressionEventEnrichmentApp")
    .master(SparkUtils.sparkMaster)
    .getOrCreate()

  new EtlJob(
    new ImpressionEventExtractor(spark),
    new EnrichEventWithSessionInfoTransformation(argsParser.sessionExpirationThresholdSeconds),
    DataFrameLoader.factory
  ).run(jobDefinition)
}
