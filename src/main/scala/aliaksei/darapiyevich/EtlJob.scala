package aliaksei.darapiyevich

import aliaksei.darapiyevich.EtlJob.LoadFactory
import org.apache.spark.sql.Dataset

class EtlJob[I, O](
                    extract: Extract[I],
                    transform: Transform[I, O],
                    load: LoadFactory[O]
                  ) {
  def run(jobDefinition: JobDefinition): Unit = {
    import jobDefinition._

    val input = extract from inputDefinition
    val transformed = transform(input)
    load(transformed) to outputDefinition
  }
}

trait Extract[E] {
  def from(inputDefinition: InputDefinition): Dataset[E]
}

trait Transform[I, O] extends (Dataset[I] => Dataset[O])

trait Load[E] {
  def to(outputDefinition: OutputDefinition): Unit
}

object EtlJob {
  type LoadFactory[E] = Dataset[E] => Load[E]
}