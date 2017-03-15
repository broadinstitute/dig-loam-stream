package loamstream.util

/**
 * @author clint
 * date: Apr 26, 2016
 */
object Functions {
  def memoize[A, B](f: A => B): A => B = {  
    val cache: ValueBox[Map[A, B]] = ValueBox(Map.empty)
    
    a => {
      cache.getAndUpdate { memo =>
        memo.get(a) match {
          case Some(result) => (memo, result)
          case None => {
            val result = f(a)
            val newMemo = memo + (a -> result)
            (newMemo, result)
          }
        }
      }
    }
  }
}
