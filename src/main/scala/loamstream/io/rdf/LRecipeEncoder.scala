package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.piles.{LPile, LPileSpec}
import loamstream.model.recipes.LRecipe
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/26/2016.
  */
object LRecipeEncoder extends Encoder[LRecipe] {
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], recipe: LRecipe): IRI = {
    val iri = LIdIris.toIri(recipe)
    io.conn.add(iri, RDF.TYPE, Loam.recipe)
    io.conn.add(iri, RDF.TYPE, LKindEncoder.encode(io, recipe.spec.kind))
    val inputSpecsNode = RedFern.iterableEncoder[LPileSpec](LPileSpecEncoder).encode(io, recipe.spec.inputs)
    io.conn.add(inputSpecsNode, RDF.TYPE, Loam.seq)
    io.conn.add(iri, Loam.requiresInputs, inputSpecsNode)
    io.conn.add(iri, Loam.providesOutput, LPileSpecEncoder.encode(io, recipe.spec.output))
    val inputsNode = RedFern.iterableEncoder[LPile](LPileEncoder).encode(io, recipe.inputs)
    io.conn.add(inputsNode, RDF.TYPE, Loam.seq)
    io.conn.add(iri, Loam.hasInputs, inputsNode)
    io.conn.add(iri, Loam.hasOutput, LPileEncoder.encode(io, recipe.output))
    iri
  }
}
