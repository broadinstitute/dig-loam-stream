package tools

import java.nio.file.Path

import htsjdk.variant.variantcontext.{Genotype, VariantContext}
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
  val samples: Seq[String] = reader.getFileHeader.getGenotypeSamples.asScala.toSeq

  def rowIter: Iterator[VariantContext] = reader.iterator().asScala

  def genotypesIter: Iterator[Seq[Genotype]] = rowIter.map({ row => row.getGenotypes.asScala.toSeq })

  def genotypeMapIter: Iterator[Map[String, Genotype]] =
    rowIter.map({row => samples.zip(row.getGenotypes.asScala).toMap})


}
