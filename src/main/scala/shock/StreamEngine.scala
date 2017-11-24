package shock.streaming

trait StreamEngine {
  def setup(): Unit
  def teardown(): Unit
}
