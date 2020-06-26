package loamstream.loam.intake.aggregator

import org.scalatest.FunSuite

/**
 * @author clint
 * Jun 25, 2020
 */
final class MetadataTest extends FunSuite {
  test("escape") {
    import Metadata.escape
    
    assert(escape("") === "")
    assert(escape("foo") === "foo")
    assert(escape("foo bar") === "\"foo bar\"")
    assert(escape(" ") === "\" \"")
    
    assert(escape("foo_\"lalala\"") === "foo_\"lalala\"")
    assert(escape("foo \"lalala\"") === "\"foo \\\"lalala\\\"\"")
  }
}
