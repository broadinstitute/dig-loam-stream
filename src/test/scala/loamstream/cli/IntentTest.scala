package loamstream.cli

import org.scalatest.FunSuite
import loamstream.model.execute.HashingStrategy

/**
 * @author clint
 * Dec 13, 2017
 */
final class IntentTest extends FunSuite {
  private val exampleFile = "src/examples/loam/cp.loam"
  
  private def cliConf(argString: String): Conf = Conf(argString.split("\\s+").toSeq)
  
  test("determineHashingStrategy") { 
    {
      val conf = cliConf(s"--disable-hashing --dry-run $exampleFile")
      
      assert(Intent.determineHashingStrategy(conf) === HashingStrategy.DontHashOutputs)
    }

    {
      val conf = cliConf(s"--dry-run $exampleFile")
      
      assert(Intent.determineHashingStrategy(conf) === HashingStrategy.HashOutputs)
    }
  }
}
