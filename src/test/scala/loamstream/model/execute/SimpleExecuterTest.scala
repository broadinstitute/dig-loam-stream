package loamstream.model.execute

import scala.concurrent.ExecutionContext.Implicits.global

/**
 * @author clint
 * date: Apr 12, 2016
 */
final class SimpleExecuterTest extends ExecuterTest {
  override def makeExecuter: LExecuter = new SimpleExecuter
}