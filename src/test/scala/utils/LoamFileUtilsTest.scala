package utils

import loamstream.TestHelpers
import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Mar 10, 2016
  */
final class LoamFileUtilsTest extends FunSuite {
  test("Relative paths can be resolved properly") {
    val miniVcf = LoamFileUtils.resolveRelativePath("src/test/resources/mini.vcf")
    
    import TestHelpers.path
    
    assert(miniVcf.getFileName === path("mini.vcf"))
    assert(miniVcf.toFile.exists)
  }
  
  test("enclosed() closes things properly") {
    final class Foo {
      var isClosed = false
      
      def close(): Unit = isClosed = true
    }
    
    implicit object FoosCanBeClosed extends CanBeClosed[Foo] {
      override def close(f: Foo): Unit = f.close()
    }
    
    val foo = new Foo
    
    assert(foo.isClosed === false)
    
    val result = LoamFileUtils.enclosed(foo) { foo =>
      42
    }
    
    assert(result === 42)
    assert(foo.isClosed === true)
  }
}

