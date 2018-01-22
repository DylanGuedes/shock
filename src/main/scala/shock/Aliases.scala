import shock.engines.spark.SparkEngine
import shock.pipeline.Pipeline

package shock {
  package object aliases {
    type StringHash = Map[String, String]
    type TaskSignature = (SparkEngine, Pipeline, StringHash) => Pipeline
  }
}
