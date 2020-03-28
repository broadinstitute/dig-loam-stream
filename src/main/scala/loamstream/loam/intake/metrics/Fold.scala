package loamstream.loam.intake.metrics

import cats.kernel.Monoid
import cats.Functor
import cats.Applicative
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

abstract class Fold[E, O] {
  type M
  def m: Monoid[M]

  def tally: E => M
  def summarize: M => O
}

object Fold extends App {
  def apply[E, A, _M](_m: Monoid[_M])(
    _tally: E => _M, 
    _summarize: _M => A): Fold[E, A] = new Fold[E, A] {
    
    override type M = _M
    override val m: Monoid[M] = _m
    override val tally: E => _M = _tally
    override val summarize: M => A = _summarize
  }
  
  def fold[I, A](input: TraversableOnce[I])(f: Fold[I, A]): A = {
    val reduced = input.foldLeft(f.m.empty) { (accMonoid, elem) =>
      f.m.combine(accMonoid, f.tally(elem)) 
    }
    
    f.summarize(reduced)
  }
  
  def sumBy[M](implicit m: Monoid[M]): Fold[M, M] = Fold(m)(identity, identity)
  
  def combine[E, A1, A2](f1: Fold[E, A1], f2: Fold[E, A2]): Fold[E, (A1, A2)] = { 
    new Fold[E, (A1, A2)] {
      override type M = (f1.M, f2.M)
      override def m: Monoid[M] = new Monoid[M] {
        override def empty: (f1.M, f2.M) = (f1.m.empty, f2.m.empty)
        override def combine(l: (f1.M, f2.M), r: (f1.M, f2.M)): M = { 
          (f1.m.combine(l._1, r._1), f2.m.combine(l._2, r._2))
        }
      }
      override def tally: E => M = e => (f1.tally(e), f2.tally(e))
      override def summarize: M => (A1, A2) = m => (f1.summarize(m._1), f2.summarize(m._2))
    }
  }
  
  implicit def foldFunctor[E]: Functor[({type F[X]=Fold[E, X]})#F] = new Functor[({type F[X]=Fold[E, X]})#F] {
    override def map[T, U](f: Fold[E, T])(fn: T => U): Fold[E, U] = { 
      Fold(f.m)(f.tally, f.summarize.andThen(fn))
    }
  }
  
  implicit def foldApplicative[E]: Applicative[({type F[X]=Fold[E, X]})#F] = new Applicative[({type F[X]=Fold[E, X]})#F] {
    import cats.kernel.instances.unit.catsKernelStdAlgebraForUnit
    import cats.implicits._
    
    override def pure[X](x: X) = Fold(catsKernelStdAlgebraForUnit)(_ => (), _ => x)
    override def ap[X, Y](ff: Fold[E, (X) => Y])(fa: Fold[E, X]): Fold[E, Y] = {
      Fold(ff.m product fa.m)(
        i => (ff.tally(i), fa.tally(i)),
        { case (mf, ma) => ff.summarize(mf)(fa.summarize(ma)) }
      )
    }
  }

  def count[A]: Fold[A, Int] = Fold(Monoids.addition[Int])(_ => 1, identity)
  
  def countIf[A](p: A => Boolean): Fold[A, Int] = Fold(Monoids.addition[Int])(a => if(p(a)) 1 else 0, identity)
}
