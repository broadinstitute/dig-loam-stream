package tools

import java.nio.file.Path
import utils.Loggable

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
    var exitCode = 1
    if (command.nonEmpty) {
      trace("Executing job " + command)
      exitCode = command.!
      trace("Job finished!")
    } else {
      error("Command wasn't specified")
    }
    exitCode
  }

  def importVcf(vcf: Path, vds: Path): Int = {
    val vcfFile = vcf.toAbsolutePath.toString
    val vdsFile = vds.toAbsolutePath.toString
    val command = Seq(hail, "importvcf", "-f", vcfFile, "splitmulti", "write", "-o", vdsFile)

    execute(command)
  }

  def calculateSingletons(vds: Path, singletons: Path): Int = {
    val vdsFile = vds.toAbsolutePath.toString
    val singletonFile = singletons.toAbsolutePath.toString
    val command = Seq(hail, "read", "-i", vdsFile, "sampleqc", "exportsamples", "-o", singletonFile, "-c",
      "SAMPLE=s.id, SINGLETONS=sa.qc.nSingleton")

    execute(command)
  }
}

