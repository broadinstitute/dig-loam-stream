package loamstream.conf

import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory

/**
 * @author clint
 * May 4, 2017
 */
final class DynamicConfigTest extends FunSuite {
  import DynamicConfigTest._  
  
  private def parse(s: String) = ConfigFactory.parseString(s)
  
  test("selectDynamic/as") {
    val conf = DynamicConfig(parse("foo { bar { baz = 42 }, nuh = [4,5,6] }"))
    
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    
    assert(conf.foo.as[Foo] === Foo(Bar(42), Seq(4,5,6)))
    
    assert(conf.foo.bar.baz.as[Int] === 42)
    
    assert(conf.foo.nuh.as[Seq[Int]] === Seq(4,5,6))
    
    //type mismatch
    intercept[Exception] {
      conf.foo.bar.as[Seq[Int]]
    }

    //type mismatch
    intercept[Exception] {
      conf.foo.bar.as[Int]
    }
    
    //type mismatch
    intercept[Exception] {
      conf.as[String]
    }
    
    //nonexistent key
    intercept[Exception] {
      conf.foo.asdf.as[String]
    }
  }
  
  test("selectDynamic/unpack") {
    val conf = DynamicConfig(parse("foo { bar { baz = 42 }, nuh = [4,5,6], zuh = hello }"))
    
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    
    assert(conf.foo.bar.baz.unpack === 42)
    assert(conf.foo.zuh.unpack === "hello")
    
    //unsupported
    intercept[Exception] {
      conf.foo.bar.unpack
    }

    //unsupported
    intercept[Exception] {
      conf.foo.nuh.unpack
    }
  }
}

object DynamicConfigTest {
  final case class Bar(baz: Int)
  
  final case class Foo(bar: Bar, nuh: Seq[Int])
}
