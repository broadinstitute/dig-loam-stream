package loamstream.util

/**
 * @author clint
 * date: Apr 26, 2016
 */
object Functions {
  def memoize[A, B](f: A => B): A => B = {  
    var memo: Map[A, B] = Map.empty
    
    a => {
      memo.get(a) match {
        case Some(result) => result
        case None => {
          val result = f(a)
          memo += (a -> result)
          result
        }
      }
    }
  }
}