package loamstream.util

/**
 * @author clint
 * date: Apr 26, 2016
 */
object Functions {
  def memoize[A](f: () => A): () => A = {
    val cached: ValueBox[Option[A]] = ValueBox(None)
    
    () => {
      val (newValue, _) = cached.mutateAndGet { oldValue =>
        if(oldValue.isDefined) oldValue else Some(f())
      }
      
      newValue.get
    }
  }
  
  def memoize[A, B](f: A => B, shouldCache: B => Boolean = (_ :B) => true): A => B = {
    val cache: ValueBox[Map[A, B]] = ValueBox(Map.empty)
    
    a => {
      cache.getAndUpdate { memo =>
        memo.get(a) match {
          case Some(result) => (memo, result)
          case None => {
            val result = f(a)
            
            if(shouldCache(result)) {
              val newMemo = memo + (a -> result)
  
              (newMemo, result)
            } else {
              (memo, result)
            }
          }
        }
      }
    }
  }
}
