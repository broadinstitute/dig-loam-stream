package loamstream.util

import loamstream.util.DepositBox.Receipt
import org.scalatest.FunSuite

/** Test of DepositBox */
class DepositBoxTest extends FunSuite {

  def assertHas[A](box: DepositBox[A], receipt: Receipt, a: A): Unit = {
    assert(box.contains(receipt))
    assert(box.get(receipt).contains(a))
    assert(box(receipt) === a)
  }

  def assertHasNot[A](box: DepositBox[A], receipt: Receipt): Unit = {
    assert(!box.contains(receipt))
    assert(box.get(receipt).isEmpty)
    assertThrows[NoSuchElementException](box(receipt))
  }

  test("deposit, size, contains, get, apply, remove") {
    val box = DepositBox.empty[String]
    for(i1 <- 1 until 3) {
      assert(box.size === 0)
      val yo = "Yo!"
      val receiptYo = box.deposit(yo)
      assert(box.size === 1)
      assertHas(box, receiptYo, yo)
      val hey = "Hey!"
      val receiptHey = box.deposit(hey)
      assert(box.size === 2)
      assertHas(box, receiptYo, yo)
      assertHas(box, receiptHey, hey)
      val hello = "Hello!"
      val receiptHello = box.deposit(hello)
      assert(box.size === 3)
      assertHas(box, receiptYo, yo)
      assertHas(box, receiptHey, hey)
      assertHas(box, receiptHello, hello)
      for (i2 <- 1 until 3) {
        box.remove(receiptYo)
        assert(box.size === 2)
        assertHasNot(box, receiptYo)
        assertHas(box, receiptHey, hey)
        assertHas(box, receiptHello, hello)
      }
      box.remove(receiptHey)
      assert(box.size === 1)
      assertHasNot(box, receiptYo)
      assertHasNot(box, receiptHey)
      assertHas(box, receiptHello, hello)
      box.remove(receiptHey)
      assert(box.size === 1)
      assertHasNot(box, receiptYo)
      assertHasNot(box, receiptHey)
      assertHas(box, receiptHello, hello)
      box.remove(receiptHello)
      assert(box.size === 0)
      assertHasNot(box, receiptYo)
      assertHasNot(box, receiptHey)
      assertHasNot(box, receiptHello)
    }
  }

}
