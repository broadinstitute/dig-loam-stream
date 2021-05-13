package loamstream.drm

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.drm.uger.UgerDefaults
import loamstream.model.execute.DrmSettings

import scala.collection.compat._

/**
 * @author clint
 * May 25, 2018
 */
final class DrmSystemTest extends FunSuite {
  import DrmSystem.Lsf
  import DrmSystem.Uger
  
  test("defaultQueue") {
    assert(Uger.defaultQueue === Some(UgerDefaults.queue))
    assert(Lsf.defaultQueue === None)
  }
  
  test("config") {
    TestHelpers.withScriptContext { implicit scriptContext =>
      assert(Uger.config(scriptContext) === scriptContext.ugerConfig)
      assert(Lsf.config(scriptContext) === scriptContext.lsfConfig)
    }
  }
  
  test("settingsFromConfig") {
    TestHelpers.withScriptContext { implicit scriptContext =>
      assert(Uger.settingsFromConfig(scriptContext) === DrmSettings.fromUgerConfig(scriptContext.ugerConfig))
      assert(Lsf.settingsFromConfig(scriptContext) === DrmSettings.fromLsfConfig(scriptContext.lsfConfig))
    }
  }
  
  test("fromName") {
    def doTest(name: String, expected: Option[DrmSystem]): Unit = {
      def to1337Case(s: String): String = {
        val tuples: Seq[((Char, Char), Int)] = s.toLowerCase.zip(s.toUpperCase).zipWithIndex
        
        val chars = tuples.map { case ((lc, uc), i) => if (i % 2 == 0) lc else uc }
        
        chars.foldLeft("") { _ + _ }
      }
      
      assert(DrmSystem.fromName(name) === expected)
      assert(DrmSystem.fromName(name.toUpperCase) === expected)
      assert(DrmSystem.fromName(name.toLowerCase) === expected)
      assert(DrmSystem.fromName(to1337Case(name)) === expected)
    }
    
    doTest("uger", Some(Uger))
    doTest("lsf", Some(Lsf))
      
    doTest("", None)
    doTest("uger1234", None)
    doTest("asdasdasdasd", None)
    
    assert(DrmSystem.fromName("Uger") === Some(Uger))
    assert(DrmSystem.fromName("Lsf") === Some(Lsf))
  }
  
  test("values") {
    assert(DrmSystem.values.to(Set) === Set(Uger, Lsf))
  }
}
