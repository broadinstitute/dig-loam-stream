package loamstream.tools

import loamstream.util.Loggable
import sys.process._

/**
 * Created on: 1/20/16
 *
 * @author Kaan Yuksel
 */
object ImputationTools extends Loggable {
  def execute(command: Seq[String]): Int = {
    def logAndProduce[A](f: => A): A = {
      try {
        trace("Executing job " + command)
        f
      } finally {
        trace("Job finished!")
      }
    }
    
    command match {
      case Nil => error("Command wasn't specified") ; 1  
      case _ => logAndProduce(command.!)
    }
  }

  def getShapeItCmdLine(shapeItBinary: String, vcf: String, map: String, haps: String, samples: String, log: String,
                        numThreads: Int = 1): Seq[String] = {
    Seq(shapeItBinary, "-V", vcf, "-M", map, "-O", haps, samples, "-L", log, "--thread", numThreads.toString)
  }
}

