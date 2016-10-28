package loamstream.util

import org.scalatest.FunSuite
import java.time.Instant
import java.io.StringReader

/**
 * @author clint
 * Oct 28, 2016
 */
final class VersionsTest extends FunSuite {
  
  private val oct28th = Instant.parse("2016-10-28T18:52:40.889Z")
  private val oct29th = Instant.parse("2016-10-29T18:52:40.889Z")
  
  test("happy path") {
    //NB: Use versionInfo.properties from src/test/scala, which takes priority at test-time
    val versions = Versions.load().get
    
    assert(versions.name === "foo")
    assert(versions.version === "bar")
    assert(versions.branch === "baz")
    assert(versions.lastCommit === Some("blerg"))
    assert(versions.anyUncommittedChanges === true)
    assert(versions.describedVersion === Some("nuh"))
    assert(versions.buildDate === oct28th)
    
    val expected = s"foo bar (nuh) branch: baz commit: blerg (PLUS uncommitted changes!) built on: $oct28th"
    
    assert(versions.toString === expected)
  }
  
  test("happy path - uncommittedChanges is false") {
    val data = new StringReader("""
      name=foo
      version=bar
      branch=baz
      lastCommit=blerg
      uncommittedChanges=false
      describedVersion=nuh
      buildDate=2016-10-29T18:52:40.889Z""")
    
    val versions = Versions.loadFrom(data).get
    
    assert(versions.name === "foo")
    assert(versions.version === "bar")
    assert(versions.branch === "baz")
    assert(versions.lastCommit === Some("blerg"))
    assert(versions.anyUncommittedChanges === false)
    assert(versions.describedVersion === Some("nuh"))
    assert(versions.buildDate === oct29th)
    
    val expected = s"foo bar (nuh) branch: baz commit: blerg built on: $oct29th"
    
    assert(versions.toString === expected)
  }
  
  test("no last commit") {
    val data = new StringReader("""
      name=foo
      version=bar
      branch=baz
      lastCommit=
      uncommittedChanges=true
      describedVersion=nuh
      buildDate=2016-10-28T18:52:40.889Z""")
    
    val versions = Versions.loadFrom(data).get
    
    assert(versions.name === "foo")
    assert(versions.version === "bar")
    assert(versions.branch === "baz")
    assert(versions.lastCommit === None)
    assert(versions.anyUncommittedChanges === true)
    assert(versions.describedVersion === Some("nuh"))
    assert(versions.buildDate === oct28th)
    
    val expected = s"foo bar (nuh) branch: baz commit: UNKNOWN (PLUS uncommitted changes!) built on: $oct28th"
    
    assert(versions.toString === expected)
  }
  
  test("no described version") {
    val data = new StringReader("""
      name=foo
      version=bar
      branch=baz
      lastCommit=blerg
      uncommittedChanges=true
      describedVersion=
      buildDate=2016-10-28T18:52:40.889Z""")
    
    val versions = Versions.loadFrom(data).get
    
    assert(versions.name === "foo")
    assert(versions.version === "bar")
    assert(versions.branch === "baz")
    assert(versions.lastCommit === Some("blerg"))
    assert(versions.anyUncommittedChanges === true)
    assert(versions.describedVersion === None)
    assert(versions.buildDate === oct28th)
    
    val expected = s"foo bar (UNKNOWN) branch: baz commit: blerg (PLUS uncommitted changes!) built on: $oct28th"
    
    assert(versions.toString === expected)
  }
  
  test("no optional fields") {
    val data = new StringReader("""
      name=foo
      version=bar
      branch=baz
      lastCommit=
      uncommittedChanges=true
      describedVersion=
      buildDate=2016-10-28T18:52:40.889Z""")
    
    val versions = Versions.loadFrom(data).get
    
    assert(versions.name === "foo")
    assert(versions.version === "bar")
    assert(versions.branch === "baz")
    assert(versions.lastCommit === None)
    assert(versions.anyUncommittedChanges === true)
    assert(versions.describedVersion === None)
    assert(versions.buildDate === oct28th)
    
    val expected = s"foo bar (UNKNOWN) branch: baz commit: UNKNOWN (PLUS uncommitted changes!) built on: $oct28th"
    
    assert(versions.toString === expected)
  }
  
  test("missing field") {
    val data = new StringReader("""
      name=foo
      sjadghasdjhasdg=bar
      branch=baz
      lastCommit=
      uncommittedChanges=true
      describedVersion=
      buildDate=2016-10-28T18:52:40.889Z""")
    
    assert(Versions.loadFrom(data).isFailure)
  }
  
  test("junk input") {
    assert(Versions.loadFrom(new StringReader("kashdkjasdhkjasdh")).isFailure)
    assert(Versions.loadFrom(new StringReader("")).isFailure)
    assert(Versions.loadFrom(new StringReader("   ")).isFailure)
  }
}