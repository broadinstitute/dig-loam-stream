package tools

import java.io.{File, PrintWriter}
import java.nio.file.Path

import htsjdk.variant.vcf.VCFFileReader
import loamstream.conf.SampleFiles
import utils.{FileUtils, Loggable}

import scala.collection.JavaConverters.asScalaBufferConverter

/**
  * Created on: 1/20/16
  *
  * @author Kaan Yuksel
  */
class VcfParser {
  def newVcfFileReader(path: Path, requireIndex: Boolean = false): VCFFileReader =
    new VCFFileReader(path.toFile, requireIndex)

  def readSamples(reader: VCFFileReader): Seq[String] = reader.getFileHeader.getGenotypeSamples.asScala.toSeq

  def readSamples(path: Path, requireIndex: Boolean = false): Seq[String] =
    readSamples(newVcfFileReader(path, requireIndex))

  def printToFile(f: File)(op: PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    FileUtils.enclosed(p)(p.close)(op)
  }
}

object SampleExtractorApp extends App with Loggable {
  val vcfParser = new VcfParser
  (SampleFiles.miniVcfOpt, SampleFiles.samplesOpt) match {
    case (Some(vcfFile), Some(samplesFile)) =>
      val samples = vcfParser.readSamples(vcfFile)
      vcfParser.printToFile(new File("samples.txt")) {
        p => samples.foreach(p => debug(p.toString))
      }
    case _ => ()
  }
}
