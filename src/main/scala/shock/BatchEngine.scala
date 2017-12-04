package shock.batch

trait BatchEngine {
  def setup(): Unit
  def teardown(): Unit
}

trait IngestionStrategy {
}
