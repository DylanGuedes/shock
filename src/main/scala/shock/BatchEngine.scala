package shock.batch

import org.apache.spark.sql.SparkSession.Builder

trait BatchEngine {
  def setup(): Unit
  def teardown(): Unit
}

trait IngestionStrategy {
  def setup(): Unit
  def configSession(session: Builder): Builder
}
