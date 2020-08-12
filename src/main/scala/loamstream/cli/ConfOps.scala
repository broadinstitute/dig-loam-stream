package loamstream.cli

import loamstream.drm.DrmSystem
import scala.annotation.tailrec

/**
 * @author clint
 * Aug 11, 2020
 */
object ConfOps {
  final implicit class ConfModifiers(val conf: Conf) extends AnyVal { 
    def withBackend(newBackend: String): Conf = {
      val knownDrmBackends: Set[String] = DrmSystem.values.map(_.name.toLowerCase).toSet
      
      if(knownDrmBackends.contains(newBackend)) {
        @tailrec
        def loop(remaining: Seq[String], acc: Seq[String]): Seq[String] = {
          if(remaining.isEmpty) { acc }
          else if(remaining.head == "--backend") {
            loop(remaining.drop(2), acc ++ Seq("--backend", newBackend)) 
          } else {
            loop(remaining.tail, acc :+ remaining.head)
          }
        }
        
        Conf(loop(conf.arguments, Nil))
      } else {
        sys.error("Flerg")
      }
    }
  }
}
