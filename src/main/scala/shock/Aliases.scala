import shock.engines.SparkEngine
import shock.Pipeline

import play.api.libs.json.{JsValue}

package shock {
  package object aliases {
    type StringHash = Map[String, String]
    type TaskSignature = (SparkEngine, Pipeline, JsValue) => Pipeline
  }
}
