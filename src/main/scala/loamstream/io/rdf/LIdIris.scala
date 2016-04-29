package loamstream.io.rdf

import loamstream.model.{LId, Store, Tool}
import org.openrdf.model.IRI
import org.openrdf.model.impl.SimpleValueFactory

/**
  * LoamStream
  * Created by oliverr on 4/26/2016.
  */
object LIdIris {

  val baseNameSpace = "http://www.broadinstitute.org/LoamStream/instances/"
  val storesNamespace = baseNameSpace + "stores#"
  val toolsNamespace = baseNameSpace + "tools#"


  def namespace(idOwner: LId.Owner): String = idOwner match {
    case store: Store => storesNamespace
    case tool: Tool => toolsNamespace
  }

  val factory = SimpleValueFactory.getInstance()

  def toIri(idOwner: LId.Owner): IRI = factory.createIRI(s"${namespace(idOwner)}${idOwner.id.name}")

  def fromIri(iri: IRI): LId = LId.fromName(iri.getLocalName)

}
