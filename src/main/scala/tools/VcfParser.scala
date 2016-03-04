package tools

import java.io.{BufferedReader, File, FileInputStream, PrintWriter}
import java.nio.file.Path
import java.util.zip.GZIPInputStream

import htsjdk.variant.vcf.VCFFileReader

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.{BufferedSource, Source}

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

  def getSamples(header: String): Array[String] = {
    val ignoreable = "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\t"
    header.stripPrefix(ignoreable).split("\t")
  }

  def gzBufferedReader(gzFile: String): BufferedReader = gzBufferedSource(gzFile).bufferedReader()

  def gzBufferedSource(gzFile: String): BufferedSource =
    Source.fromInputStream(new GZIPInputStream(new FileInputStream(gzFile)))

  def versionSupported(versionLine: String, versionSupported: String): Boolean = {
    val expectedLine = "##fileformat=VCFv" + versionSupported
    versionLine == expectedLine
  }

  def getHeaderLine(buffer: BufferedReader): String = {
    var line: String = ""
    while ( {
      line = buffer.readLine();
      line != null
    })
      if (!line.startsWith("##")) {
        // skip metalines
        return line
      }
    line
  }

  def printToFile(f: File)(op: PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }
}

object SampleExtractorApp extends App {
  val vcfParser = new VcfParser
  val versionSupported = "4.1"
  val file = "/Users/kyuksel/BurdenFiles/v3.clean.1000.vcf.gz"
  val buffer = vcfParser.gzBufferedReader(file)
  if (!vcfParser.versionSupported(buffer.readLine(), versionSupported)) {
    println("VCF versions other than " + versionSupported + " are not supported")
    System.exit(1)
  }

  val headerLine = vcfParser.getHeaderLine(buffer)
  val samples = vcfParser.getSamples(headerLine)
  vcfParser.printToFile(new File("samples.txt")) {
    p => samples.foreach(p.println)
  }
}
