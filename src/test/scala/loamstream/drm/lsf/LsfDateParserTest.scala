package loamstream.drm.lsf

import java.time.Instant
import java.time.LocalDateTime

import org.scalatest.FunSuite

/**
 * @author clint
 * May 22, 2018
 */
final class LsfDateParserTest extends FunSuite {
  test("toInstant") {
    val currentYear = LocalDateTime.now.getYear
    
    val lsfTimeString = "May 22 15:58"
    
    val isoTimeString = s"${currentYear}-05-22T15:58:00.000Z"
    
    val expected = Instant.parse(isoTimeString)
    
    assert(LsfDateParser.toInstant(lsfTimeString) === Some(expected)) 
  }
  
  test("toInstant - trailing 'L'") {
    val currentYear = LocalDateTime.now.getYear
    
    val lsfTimeString = "May 22 15:58 L"
    
    val isoTimeString = s"${currentYear}-05-22T15:58:00.000Z"
    
    val expected = Instant.parse(isoTimeString)
    
    assert(LsfDateParser.toInstant(lsfTimeString) === Some(expected)) 
  }
}
