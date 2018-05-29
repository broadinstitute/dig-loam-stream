package loamstream.model.execute

import org.scalatest.FunSuite

/**
 * @author clint
 * Oct 12, 2017
 */
final class EnvironmentTypeTest extends FunSuite {
  import EnvironmentType._
  
  test("name") {
    assert(Local.name === "local")
    assert(Google.name === "google")
    assert(Uger.name === "uger")
    assert(Lsf.name === "lsf")
  }
  
  test("is* predicates") {
    assert(Local.isLocal === true)
    assert(Local.isGoogle === false)
    assert(Local.isUger === false)
    assert(Local.isLsf === false)
    
    assert(Google.isLocal === false)
    assert(Google.isGoogle === true)
    assert(Google.isUger === false)
    assert(Google.isLsf === false)
    
    assert(Uger.isLocal === false)
    assert(Uger.isGoogle === false)
    assert(Uger.isUger === true)
    assert(Uger.isLsf === false)
    
    assert(Lsf.isLocal === false)
    assert(Lsf.isGoogle === false)
    assert(Lsf.isUger === false)
    assert(Lsf.isLsf === true)
  }
  
  test("fromString") {
    assert(fromString("") === None)
    assert(fromString("   ") === None)
    assert(fromString("asdff") === None)
    
    assert(fromString("local") === Some(Local))
    assert(fromString("Local") === Some(Local))
    assert(fromString("LOCAL") === Some(Local))
    assert(fromString("LoCaL") === Some(Local))
    
    assert(fromString("google") === Some(Google))
    assert(fromString("Google") === Some(Google))
    assert(fromString("GOOGLE") === Some(Google))
    assert(fromString("GoOgLe") === Some(Google))
    
    assert(fromString("uger") === Some(Uger))
    assert(fromString("Uger") === Some(Uger))
    assert(fromString("UGER") === Some(Uger))
    assert(fromString("UgEr") === Some(Uger))
    
    assert(fromString("lsf") === Some(Lsf))
    assert(fromString("Lsf") === Some(Lsf))
    assert(fromString("LSF") === Some(Lsf))
    assert(fromString("LsF") === Some(Lsf))
  }
}
