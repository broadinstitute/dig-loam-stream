package loamstream

import org.scalatest.FunSuite
import loamstream.loam.LoamScriptContext
import java.nio.file.Path
import org.apache.commons.io.FileUtils

/**
 * @author clint
 * Jun 19, 2018
 */
trait LoamFunSuite extends FunSuite {
  def testWithScriptContext(name: String)(body: LoamScriptContext => Any): Unit = {
    test(name) {
      TestHelpers.withScriptContext(body)
    }
  }
  
  def testWithWorkDir(name: String)(body: Path => Any): Unit = {
    test(name) {
      TestHelpers.withWorkDir(getClass.getSimpleName)(body)
    }
  }
}
