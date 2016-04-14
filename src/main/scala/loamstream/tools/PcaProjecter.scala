package loamstream.tools

import htsjdk.variant.variantcontext.Genotype
import VcfParser.MapRow

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 3/10/16.
  */
object PcaProjecter {
  def findNPcaMax(weights: Map[String, Seq[Double]]): Int = weights.mapValues(_.size).values.min

  def apply(weights: Map[String, Seq[Double]]): PcaProjecter = PcaProjecter(weights, findNPcaMax(weights))
}

case class PcaProjecter(weights: Map[String, Seq[Double]], nPcaMax: Int) {

  def addPcas(pcas1: Seq[Seq[Double]], pcas2: Seq[Seq[Double]]): Seq[Seq[Double]] =
    pcas1.zip(pcas2).map { case (samplePcas1, samplePcas2) =>
      samplePcas1.zip(samplePcas2).map { case (samplePca1, samplePca2) => samplePca1 + samplePca2 }
    }

  def project(variant: String, samples: Seq[String], row: MapRow, genotypeToDouble: Genotype => Double,
              nPca: Int): Seq[Seq[Double]] = {
    weights.get(variant) match {
      case Some(variantWeights) => {
        samples.map(row.genotypesMap).map(genotypeToDouble).map { xSample => 
          variantWeights.take(nPca).map(_ * xSample)
        }
      }
      case None => Seq.fill(samples.size, nPca)(0.0)
    }
  }

  def project(samples: Seq[String], rowIter: Iterator[MapRow], genotypeToDouble: Genotype => Double,
              nPca: Int = nPcaMax): Seq[Seq[Double]] = {
    val nSamples = samples.size
    var pcas = Seq.fill(nSamples, nPca)(0.0)
    for (row <- rowIter) {
      val variantPcas = project(row.id, samples, row, genotypeToDouble, nPca)
      pcas = addPcas(pcas, variantPcas)
    }
    pcas
  }

}
