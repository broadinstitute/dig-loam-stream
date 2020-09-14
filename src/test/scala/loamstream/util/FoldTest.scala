package loamstream.util

import org.scalatest.FunSuite
import loamstream.drm.uger.QacctTestHelpers
import java.time.LocalDateTime


/**
 * @author clint
 * Aug 3, 2020
 */
final class FoldTest extends FunSuite {
  test("map") {
    val f = Fold.countIf[Int](_ % 2 == 0).map(_.toString)
    
    assert(f.process(Seq(1, 3, 5)) === "0")
    assert(f.process(Seq(1, 2, 3, 4, 5)) === "2")
    assert(f.process(Nil) === "0")
  }
  
  test("|+| / combine") {
    val f1: Fold[Int, Int, Int] = Fold.countIf[Int](_ % 2 == 0)
    val f2: Fold[Int, String, String] = Fold("", (acc, e) => s"${acc}${e}", identity)
    
    val combined = f1 |+| f2
    
    assert(combined.process(Seq(1, 3, 5)) === (0, "135"))
    assert(combined.process(Seq(1, 2, 3, 4, 5)) === (2, "12345"))
    assert(combined.process(Nil) === (0, ""))
  }
  
  test("|+| / combine - input is traversed only once") {
    val f1: Fold[Int, Int, Int] = Fold.countIf[Int](_ % 2 == 0)
    val f2: Fold[Int, String, String] = Fold("", (acc, e) => s"${acc}${e}", identity)
    
    val combined = f1 |+| f2
    
    val is = Iterator(1, 2, 3, 4, 5)
    
    assert(combined.process(is) === (2, "12345"))
  }
  
  test("sum") {
    val f = Fold.sum[Int]
    
    assert(f.process(Seq(1, 3, 5)) === 9)
    assert(f.process(Seq(1, 3, 4, 5)) === 13)
    assert(f.process(Seq(2, 4, 6)) === 12)
    assert(f.process(Nil) === 0)
  }
  
  test("count") {
    val f = Fold.count[Int]
    
    assert(f.process(Seq(1, 3, 5)) === 3)
    assert(f.process(Seq(1, 3, 4, 5)) === 4)
    assert(f.process(Seq(2, 4, 6)) === 3)
    assert(f.process(Nil) === 0)
  }
  
  test("countIf") {
    val f = Fold.countIf[Int](_ % 2 == 0)
    
    assert(f.process(Seq(1, 3, 5)) === 0)
    assert(f.process(Seq(1, 3, 4, 5)) === 1)
    assert(f.process(Seq(2, 4, 6)) === 3)
    assert(f.process(Nil) === 0)
  }
  
  test("findFirst") {
    val f = Fold.findFirst[Int](_ % 2 == 0)
    
    assert(f.process(Seq(1, 3, 5)) === None)
    assert(f.process(Seq(1, 3, 4, 5)) === Some(4))
    assert(f.process(Seq(2, 4, 6)) === Some(2))
    assert(f.process(Nil) === None)
  }
  
  test("matchFirst") {
    val f = Fold.matchFirst("exit_status\\s+(.+?)$".r)
    
    val qacctOutput = QacctTestHelpers.actualQacctOutput(None, None, LocalDateTime.now, LocalDateTime.now, 42)
    
    assert(f.process(qacctOutput) === Seq("42"))
    
    assert(f.process(Nil) === Nil)
    
    assert(f.process(Seq("a", "b", "c")) === Nil)
  }
  
  test("matchFirst1") {
    val f = Fold.matchFirst1("exit_status\\s+(.+?)$".r)
    
    val qacctOutput = QacctTestHelpers.actualQacctOutput(None, None, LocalDateTime.now, LocalDateTime.now, 42)
    
    assert(f.process(qacctOutput) === Some("42"))
  }
}
