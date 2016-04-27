package loamstream.io.rdf

import loamstream.model.id.LId
import loamstream.model.jobs.tools.LTool
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import loamstream.model.stores.LStore
import org.openrdf.model.IRI
import org.openrdf.model.impl.SimpleValueFactory

/**
  * LoamStream
  * Created by oliverr on 4/26/2016.
  */
object LIdIris {

  val baseNameSpace = "http://www.broadinstitute.org/LoamStream/instances/"
  val pilesNamespace = baseNameSpace + "piles#"
  val recipesNamespace = baseNameSpace + "recipes#"
  val storesNamespace = baseNameSpace + "stores#"
  val toolsNamespace = baseNameSpace + "tools#"


  def namespace(idOwner: LId.Owner): String = idOwner match {
    case pile: LPile => pilesNamespace
    case recipe: LRecipe => recipesNamespace
    case store: LStore => storesNamespace
    case tool: LTool => toolsNamespace
  }

  val factory = SimpleValueFactory.getInstance()

  def toIri(idOwner: LId.Owner): IRI = factory.createIRI(s"${namespace(idOwner)}${idOwner.id.name}")

  def fromIri(iri: IRI): LId = LId.fromName(iri.getLocalName)

}
