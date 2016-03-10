package tools

import htsjdk.variant.variantcontext.Genotype
import tools.VcfParser.MapRow

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 3/10/16.
  */
case class PcaProjecter(weights: Map[String, Seq[Double]], nPcaMax: Int) {

  def project(rowIter: Iterator[MapRow], genotypeToDouble: Genotype => Double,
              nPca: Int = nPcaMax): Seq[Seq[Double]] = {

    ??? // TODO
  }

}
