package tools

import htsjdk.variant.variantcontext.Genotype

import scala.collection.JavaConverters.asScalaBufferConverter

/**
  * LoamStream
  * Created by oliverr on 3/7/2016.
  */
object VcfUtils {

  def genotypeToAltCount(genotype: Genotype): Int = genotype.getAlleles.asScala.count(_.isNonReference)

}
