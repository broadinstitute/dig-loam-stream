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

  def assertHasThese[A](box: DepositBox[A], items: Map[Receipt, A]): Unit = {
    for ((receipt, item) <- items) {
      assertHas(box, receipt, item)
    }
    assert(box.size === items.size)
  }

  def assertHasNotThese[A](box: DepositBox[A], receipts: Set[Receipt]): Unit = {
    for (receipt <- receipts) {
      assertHasNot(box, receipt)
    }
  }

  test("deposit, size, contains, get, apply, remove") {
    val box = DepositBox.empty[String]
    var addedItems: Map[Receipt, String] = Map.empty
    var removedItems: Set[Receipt] = Set.empty
    def assertItemsInBox(): Unit = {
      assertHasThese(box, addedItems)
      assertHasNotThese(box, removedItems)
    }
    def addItem(item: String): Receipt = {
      val receipt = box.deposit(item)
      addedItems += receipt -> item
      removedItems -= receipt
      assertItemsInBox()
      receipt
    }
    def removeItem(receipt: Receipt): Unit = {
      box.remove(receipt)
      addedItems -= receipt
      removedItems += receipt
      assertItemsInBox()
    }
    assertItemsInBox()
    for (i1 <- 1 until 3) {
      val receiptYo = addItem("Yo!")
      val receiptHey = addItem("Hey!")
      val receiptHello1 = addItem("Hello!")
      val receiptHello2 = addItem("Hello!")
      val receiptHello3 = addItem("Hello!")
      removeItem(receiptHey)
      removeItem(receiptHey)
      removeItem(receiptHey)
      removeItem(receiptHello1)
      removeItem(receiptHello2)
      removeItem(receiptHello3)
      val receiptHelloAgain = addItem("Hello!")
      removeItem(receiptYo)
      removeItem(receiptHelloAgain)
      removeItem(receiptYo)
      removeItem(receiptHelloAgain)
    }
  }
  test("Receipt.toScalaCode"){
    for(id <- Seq(0L, 1L, 2L, 42L, -1L, -100L, Long.MinValue, Long.MaxValue)) {  // scalastyle:ignore magic.number
      assert(Receipt(id).asScalaCode === s"DepositBox.Receipt(${id}L)")
    }

  }

}
