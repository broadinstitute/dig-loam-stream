package loamstream.util

import loamstream.util.DepositBox.Receipt

/** Deposit object, get receipt, pick up later using receipt */
object DepositBox {

  case class State[A](store: Map[Receipt, A], nextId: Long) {
    def +(a: A): (State[A], Receipt) = {
      val receipt = Receipt(nextId)
      (State(store + (receipt -> a), nextId + 1), receipt)
    }

    def size: Int = store.size

    def contains(receipt: Receipt): Boolean = store.contains(receipt)

    def get(receipt: Receipt): Option[A] = store.get(receipt)

    def apply(receipt: Receipt): A = store(receipt)

    def -(receipt: Receipt): State[A] = copy(store = store - receipt)
  }

  object State {
    def empty[A]: State[A] = State(Map.empty, 0L)
  }

  def empty[A]: DepositBox[A] = DepositBox(ValueBox(State.empty))

  case class Receipt(id: Long) {
    def asScalaCode: String = s"DepositBox.Receipt(${id}L)"
  }

}

/** Deposit object, get receipt, pick up later using receipt */
case class DepositBox[A](stateBox: ValueBox[DepositBox.State[A]]) {

  def deposit(a: A): Receipt = stateBox.getAndUpdate(_ + a)

  def size: Int = stateBox.get(_.size)

  def contains(receipt: Receipt): Boolean = stateBox.get(_.contains(receipt))

  def get(receipt: Receipt): Option[A] = stateBox.get(_.get(receipt))

  def apply(receipt: Receipt): A = stateBox.get(_.apply(receipt))

  def remove(receipt: Receipt): Unit = stateBox.mutate(_ - receipt)

}
