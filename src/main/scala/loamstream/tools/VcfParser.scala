package loamstream.tools

import java.nio.file.Path
import htsjdk.variant.variantcontext.{Genotype, VariantContext}
import htsjdk.variant.vcf.VCFFileReader
import scala.collection.JavaConverters._

/**
  * Created on: 1/20/16
  *
  * @author Kaan Yuksel
  */
final class VcfParser(val reader: VCFFileReader) {
  import VcfParser._
  
  val samples: Seq[String] = reader.getFileHeader.getGenotypeSamples.asScala

  def rowIter: Iterator[RawRow] = reader.iterator.asScala.map(RawRow)

  def genotypesIter: Iterator[SeqRow] = reader.iterator.asScala.map(SeqRow(_))

  def genotypeMapIter: Iterator[MapRow] = reader.iterator.asScala.map(MapRow(_, samples))
}

object VcfParser {

  def apply(path: Path, requireIndex: Boolean = false): VcfParser = {
    new VcfParser(new VCFFileReader(path.toFile, requireIndex))
  }
  
  trait Row[T] {
    def id: String

    def data: T
  }

  final case class RawRow(context: VariantContext) extends Row[VariantContext] {
    override def id: String = context.getID

    override def data: VariantContext = context
  }

  object SeqRow {
    def apply(context: VariantContext): SeqRow = new SeqRow(context.getID, context.getGenotypes.asScala)
  }

  final class SeqRow(val id: String, val genotypes: Seq[Genotype]) extends Row[Seq[Genotype]] {
    override def data: Seq[Genotype] = genotypes
  }

  object MapRow {
    def apply(context: VariantContext, samples: Seq[String]): MapRow = {
      new MapRow(context.getID, samples.zip(context.getGenotypes.asScala).toMap)
    }
  }

  final class MapRow(val id: String, val genotypesMap: Map[String, Genotype]) extends Row[Map[String, Genotype]] {
    override def data: Map[String, Genotype] = genotypesMap
  }
}
