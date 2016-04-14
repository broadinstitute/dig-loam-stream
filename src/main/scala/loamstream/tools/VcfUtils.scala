package loamstream.tools

import htsjdk.variant.variantcontext.Genotype

/**
  * LoamStream
  * Created by oliverr on 3/7/2016.
  */
object VcfUtils {
  import scala.collection.JavaConverters._

  def genotypeToAltCount(genotype: Genotype): Int = genotype.getAlleles.asScala.count(_.isNonReference)

}
