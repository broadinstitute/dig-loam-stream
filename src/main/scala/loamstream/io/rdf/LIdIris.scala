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

  val factory = SimpleValueFactory.getInstance()

  def toIri(idOwner: LId.Owner): IRI = {
    val typeName = idOwner match {
      case pile: LPile => "piles"
      case recipe: LRecipe => "recipes"
      case store: LStore => "stores"
      case tool: LTool => "tools"
    }
    factory.createIRI(s"$baseNameSpace$typeName#${idOwner.id.name}")
  }

  def fromIri(iri: IRI): LId = LId.fromName(iri.getLocalName)

}
