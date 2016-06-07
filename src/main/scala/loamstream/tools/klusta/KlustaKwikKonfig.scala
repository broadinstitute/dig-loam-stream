package loamstream.tools.klusta

import java.nio.file.{Files, Path}
import KlustaKwikKonfig.iShankDefault

/**
  * LoamStream
  * Created by oliverr on 4/8/2016.
  */
final case class KlustaKwikKonfig(workDir: Path, fileBase: String, iShank: Int = iShankDefault) {
  def inputFileName: String = s"$fileBase.fet.$iShank"

  def inputFile: Path = workDir.resolve(inputFileName)

  def outputFileName: String = fileBase + ".clu." + iShank

  def outputFile: Path = workDir.resolve(outputFileName)
}

object KlustaKwikKonfig {
  val iShankDefault = 1
  val tempPathPrefix = "klusta"

  def withTempWorkDir(fileBase: String, iShank: Int = iShankDefault): KlustaKwikKonfig = {
    KlustaKwikKonfig(Files.createTempDirectory(tempPathPrefix), fileBase, iShank)
  }
}
