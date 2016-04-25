package loamstream.io.rdf

import loamstream.util.shot.{Miss, Shot}
import org.openrdf.model.impl.SimpleValueFactory
import org.openrdf.model.{IRI, Value}

import scala.util.Try

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

  val unknownType = iri("UnknownType")
  val hasValue = iri("hasValue")
  val hasType = iri("hasType")

  val tupleNamePrefix = "Tuple"

  def tuple(n: Int): IRI = iri(tupleNamePrefix + n)

  def isTupleType(value: Value): Boolean = {
    value match {
      case iri: IRI => (iri.getNamespace == namespace) && iri.getLocalName.startsWith(tupleNamePrefix)
      case _ => false
    }
  }

  def tupleTypeToArity(value: Value): Shot[Int] = {
    value match {
      case iri: IRI =>
        if (iri.getNamespace == namespace) {
          val name = iri.getLocalName
          if (name.startsWith(tupleNamePrefix)) {
            val arityString = name.replaceFirst(tupleNamePrefix, "")
            Shot.fromTry(Try {
              arityString.toInt
            })
          } else {
            Miss(s"Expected name to start with '$tupleNamePrefix', but got '$name'.")
          }
        } else {
          Miss(s"Expected namespace '$namespace', but got '${iri.getNamespace}'.")
        }
      case _ => Miss(s"Expected IRI, but got '$value'.")
    }
  }
}
