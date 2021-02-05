package loamstream.loam.intake.dga

object Foo extends App {
  def solution(as: Array[Int]): Int = {
    val z: (Set[Int], Int) = (Set.empty, -1)
    
    println(s"HELLO")
    
    val x = as.iterator.zipWithIndex.scanLeft(z) { (acc, t) =>
      val (a, i) = t
      
      val (seen, last) = acc
      
      if(seen.contains(a)) {
        println(s"DUPE: $a")
        
        seen -> i
      } else {
        println(s"NO DUPE: $a")
        
        (seen + a) -> last
      }
    }
    
    if(x.hasNext) { x.toSeq.last._2 }
    else { -1 }
  }
  
  println(solution(Array(2,1,3,5,3,2)))
  //println(solution("nndNfdfdf"))
}
