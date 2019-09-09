package aliaksei.darapiyevich

import org.apache.spark.sql.{DataFrame, Row}

trait CachingDataFrameExtractor extends Extract[Row] {
  var dataFrame: DataFrame = _

  abstract override def from(inputDefinition: InputDefinition): DataFrame = {
    if (dataFrame != null) {
      dataFrame
    } else {
      dataFrame = super.from(inputDefinition)
        .cache()
      dataFrame
    }
  }
}
