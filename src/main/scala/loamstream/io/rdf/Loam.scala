package loamstream.io.rdf

import org.openrdf.model.IRI
import org.openrdf.model.impl.SimpleValueFactory

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 4/20/16.
  */
object Loam {

  val namespace = "http://www.broadinstitute.org/LoamStream#"
  val prefix = "loam"
  val factory = SimpleValueFactory.getInstance()

  def add(name: String): IRI = factory.createIRI(namespace, name)

  val char = add("Char")
  val variantId = add("VariantId")
  val sampleId = add("SampleId")
  val genotype = add("Genotype")
}
