package shock.types

object StreamEngineTypes extends Enumeration {
  type StreamEngineTypes = Value
  val SparkStreaming = Value
}

object BatchEngineTypes extends Enumeration {
  type BatchEngineTypes = Value
  val Spark = Value
}

object MessageBrokerTypes extends Enumeration {
  type MessageBrokerTypes = Value
  val Kafka, RabbitMQ = Value
}

object ProcessingArchictetureType extends Enumeration {
  type ProcessingArchictetureType = Value
  val Lambda, Kappa = Value
}

object OperationStatusType extends Enumeration {
  type OperationStatusType = Value
  val SUCCESS, ERROR = Value
}
