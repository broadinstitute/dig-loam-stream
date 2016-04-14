package tools

import java.nio.file.Path
import loamstream.util.Loggable

import sys.process._

/**
 * Created on: 1/20/16
 *
 * @author Kaan Yuksel
 */
object HailTools extends Loggable {
  val hailBase = "/Users/kyuksel/GitHubClones/hail"
  val hailExecutable = "/build/install/hail/bin/hail"
  val hail = hailBase + hailExecutable

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

  private def toAbsolutePathString(p: Path): String = p.toAbsolutePath.toString
  
  def importVcf(vcf: Path, vds: Path): Int = {
    val vcfFile = toAbsolutePathString(vcf)
    val vdsFile = toAbsolutePathString(vds)
    val command = Seq(hail, "importvcf", "-f", vcfFile, "splitmulti", "write", "-o", vdsFile)

    execute(command)
  }

  def calculateSingletons(vds: Path, singletons: Path): Int = {
    val vdsFile = toAbsolutePathString(vds)
    val singletonFile = toAbsolutePathString(singletons)
    val command = Seq(hail, "read", "-i", vdsFile, "sampleqc", "exportsamples", "-o", singletonFile, "-c",
      "SAMPLE=s.id, SINGLETONS=sa.qc.nSingleton")

    execute(command)
  }
}

