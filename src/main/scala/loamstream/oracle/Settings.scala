package loamstream.oracle

import loamstream.oracle.uger.Queue

/**
 * @author clint
 * Mar 7, 2017
 */
sealed trait Settings

object Settings {
  final case class UgerSettings(queue: Queue, memory: Memory, cpus: Cpus) extends Settings {
    def nativeSpec: String = {
      //-clear -cwd -shell y -b n -q short -l h_vmem=16g
      
      val memoryPerCore = memory / cpus.value
      
      val memoryPart = s"-l h_vmem=${memoryPerCore.gb}g"
      
      val queuePart = s"-q ${queue.name}"
      
      //-binding linear:2 -pe smp 2
      val cpusPart = {
        if(cpus.isSingle) { "" }
        else { s"-binding linear:${cpus.value} -pe smp ${cpus.value}" }
      }
      
      s"-clear -cwd -shell y -b n $queuePart $memoryPart $cpusPart"
    }
  }
  
  final class LocalSettings extends Settings //TODO: What goes here?
  
  final class GoogleSettings extends Settings //TODO: What goes here? 
}

