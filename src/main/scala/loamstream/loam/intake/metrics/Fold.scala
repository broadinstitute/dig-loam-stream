package loamstream.loam.intake.metrics

import cats.kernel.Monoid
import cats.Functor
import cats.Applicative
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

/**
 * An abstract notion of folding over some collection of elements, producing a result.
 */
abstract class Fold[E, A, R] {
  def zero: A
  def add(acc: A, elem: E): A
  def summarize(accumulated: A): R
  
  def map[S](f: R => S): Fold[E, A, S] = {
    Fold(this.zero, this.add, acc => f(this.summarize(acc)))
  }
}

object Fold {
  def apply[E, A, R](
      z: => A,
      doAdd: (A, E) => A,
      doSummarize: A => R): Fold[E, A, R] = new Fold[E, A, R] {
    
    override def zero: A = z
    override def add(acc: A, elem: E): A = doAdd(acc, elem)
    override def summarize(accumulated: A): R = doSummarize(accumulated)
  }
  
  def fold[E, A, R](input: TraversableOnce[E])(f: Fold[E, A, R]): R = {
    val reduced = input.foldLeft(f.zero) { (acc, elem) =>
      f.add(acc, elem) 
    }
    
    f.summarize(reduced)
  }
  
  def combine[E, A1, A2, R1, R2](f1: Fold[E, A1, R1], f2: Fold[E, A2, R2]): Fold[E, (A1, A2), (R1, R2)] = { 
    new Fold[E, (A1, A2), (R1, R2)] {
      private type AccTuple = (A1, A2)
      private type ResultTuple = (R1, R2)
      
      override def zero: AccTuple = (f1.zero, f2.zero)
      
      override def add(accs: AccTuple, elem: E): AccTuple = {
        val (a1, a2) = accs
        
        (f1.add(a1, elem), f2.add(a2, elem))
      }
      
      override def summarize(accs: AccTuple): ResultTuple = {
        val (a1, a2) = accs

        (f1.summarize(a1), f2.summarize(a2))
      }
    }
  }
  
  def sum[E](implicit ev: Numeric[E]): Fold[E, E, E] = Fold(ev.zero, ev.plus, identity)
  
  def count[E]: Fold[E, Int, Int] = Fold(0, (acc, _) => acc + 1, identity)
  
  def countIf[E](p: E => Boolean): Fold[E, Int, Int] = Fold(0, (acc, e) => if(p(e)) acc + 1 else acc, identity)
}
