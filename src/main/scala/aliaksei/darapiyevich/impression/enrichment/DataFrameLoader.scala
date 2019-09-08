package aliaksei.darapiyevich.impression.enrichment

import aliaksei.darapiyevich.{Load, OutputDefinition}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class DataFrameLoader(
                       output: DataFrame
                     ) extends Load[Row] {

  override def to(outputDefinition: OutputDefinition): Unit = {
    output.write
      .format(outputDefinition.format)
      .save(outputDefinition.location)
  }
}