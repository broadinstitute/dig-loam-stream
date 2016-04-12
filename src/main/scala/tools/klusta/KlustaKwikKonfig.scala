package tools.klusta

import java.nio.file.{Files, Path}

import tools.klusta.KlustaKwikKonfig.iShankDefault

/**
  * LoamStream
  * Created by oliverr on 4/8/2016.
  */
object KlustaKwikKonfig {
  val iShankDefault = 1
  val tempPathPrefix = "klusta"

  def withTempWorkDir(fileBase: String, iShank: Int = iShankDefault): KlustaKwikKonfig =
    KlustaKwikKonfig(Files.createTempDirectory(tempPathPrefix), fileBase, iShank)

}

case class KlustaKwikKonfig(workDir: Path, fileBase: String, iShank: Int = iShankDefault) {
  def inputFileName: String = fileBase + ".fet." + iShank

  def inputFile: Path = workDir.resolve(inputFileName)

  def outputFileName: String = fileBase + ".clu." + iShank

  def outputFile: Path = workDir.resolve(outputFileName)
}
