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
    assert(Aws.name === "aws")
  }
  
  test("is* predicates") {
    assert(Local.isLocal === true)
    assert(Local.isGoogle === false)
    assert(Local.isUger === false)
    assert(Local.isLsf === false)
    assert(Local.isAws === false)
    
    assert(Google.isLocal === false)
    assert(Google.isGoogle === true)
    assert(Google.isUger === false)
    assert(Google.isLsf === false)
    assert(Google.isAws === false)
    
    assert(Uger.isLocal === false)
    assert(Uger.isGoogle === false)
    assert(Uger.isUger === true)
    assert(Uger.isLsf === false)
    assert(Uger.isAws === false)
    
    assert(Lsf.isLocal === false)
    assert(Lsf.isGoogle === false)
    assert(Lsf.isUger === false)
    assert(Lsf.isLsf === true)
    assert(Lsf.isAws === false)
    
    assert(Aws.isLocal === false)
    assert(Aws.isGoogle === false)
    assert(Aws.isUger === false)
    assert(Aws.isLsf === false)
    assert(Aws.isAws === true)
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
    
    assert(fromString("aws") === Some(Aws))
    assert(fromString("Aws") === Some(Aws))
    assert(fromString("AWS") === Some(Aws))
    assert(fromString("AwS") === Some(Aws))
  }
}
