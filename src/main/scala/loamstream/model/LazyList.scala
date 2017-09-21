package loamstream.model

sealed abstract class LazyList[+A] {
  def map[B](f: A => B): LazyList[B] = new LazyList.Mapped(f, this)
  
  def flatMap[B](f: A => LazyList[B]): LazyList[B] = new LazyList.FlatMapped(f, this)
  
  def filter(p: A => Boolean): LazyList[A] = new LazyList.Filtered(p, this)
  
  def foreach(f: A => Any): Unit
  
  def fold[B](z: B)(op: (B, A) => B): B = {
    var currentB = z
    
    foreach { a =>
      currentB = op(currentB, a)
    }
    
    currentB
  }
}

object LazyList {
  def apply[A](as: A*): LazyList[A] = new Literal(as: _*)
  
  private final class Literal[A](as: A*) extends LazyList[A] {
    override def foreach(f: A => Any): Unit = as.foreach(f)
  }
  
  private final class Filtered[A](predicate: A => Boolean, delegate: LazyList[A]) extends LazyList[A] {
    override def foreach(f: A => Any): Unit = delegate.foreach { a =>
      if(predicate(a)) {
        f(a)
      }
    }
  }
  
  private final class Mapped[A, B](transform: A => B, delegate: LazyList[A]) extends LazyList[B] {
    override def foreach(f: B => Any): Unit = delegate.foreach { a =>
      f(transform(a))
    }
  }
  
  private final class FlatMapped[A, B](transform: A => LazyList[B], delegate: LazyList[A]) extends LazyList[B] {
    override def foreach(f: B => Any): Unit = delegate.foreach { a =>
      transform(a).foreach(f)
    }
  }

  def main(args: Array[String]): Unit = {
    println("starting")
    
    val ll = LazyList(1,2,3).map { i => 
      println("in map") 
      i + 1
    }.flatMap { i =>
      println("in flatMap")
      LazyList(i, i)
    }.filter { i =>
      println("in filter")
      i != 3
    }
    
    println("done with combinators")
    
    ll.foreach(println)
    
    val z: Seq[Int] = Nil
    
    val is = ll.fold(z) { _ :+ _ }
    
    println(is)
  }
}
