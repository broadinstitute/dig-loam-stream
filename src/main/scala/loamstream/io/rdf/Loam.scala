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

  def iri(name: String): IRI = factory.createIRI(namespace, name)

  val char = iri("Char")
  val variantId = iri("VariantId")
  val sampleId = iri("SampleId")
  val genotype = iri("Genotype")
  val set = iri("Set")
  val seq = iri("Seq")
  val elementType = iri("elementType")
  val map = iri("Map")
  val keyType = iri("keyType")
  val valueType = iri("valueType")

  def tuple(n: Int): IRI = iri("Tuple" + n)
}
