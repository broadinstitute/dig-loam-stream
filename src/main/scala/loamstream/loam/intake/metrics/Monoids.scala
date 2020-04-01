package loamstream.loam.intake.metrics

import cats.kernel.Monoid

/**
 * @author clint
 * Mar 27, 2020
 */
object Monoids {
  //TODO: This /must/ be built into Cats somewhere
  def addition[A](implicit ev: Numeric[A]): Monoid[A] = new Monoid[A] {
    override def empty: A = ev.zero
    
    override def combine(a: A, b: A): A = ev.plus(a, b) 
  }
}
