package aliaksei.darapiyevich

import org.apache.spark.sql.{DataFrame, Row}

class DataFrameLoader(
                       output: DataFrame
                     ) extends Load[Row] {

  override def to(outputDefinition: OutputDefinition): Unit = {
    output.write
      .format(outputDefinition.format)
      .save(outputDefinition.location)
  }
}

object DataFrameLoader {
  val factory: DataFrame => DataFrameLoader = {
    new DataFrameLoader(_)
  }
}