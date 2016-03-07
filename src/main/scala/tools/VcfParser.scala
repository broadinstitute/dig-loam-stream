package tools

import java.nio.file.Path

import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFFileReader

import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter}

/**
  * Created on: 1/20/16
  *
  * @author Kaan Yuksel
  */
object VcfParser {
  def apply(path: Path, requireIndex: Boolean = false): VcfParser =
    new VcfParser(new VCFFileReader(path.toFile, requireIndex))
}

class VcfParser(val reader: VCFFileReader) {
  def readSamples: Seq[String] = reader.getFileHeader.getGenotypeSamples.asScala.toSeq

  def rowIterator: Iterator[VariantContext] = reader.iterator().asScala

}
