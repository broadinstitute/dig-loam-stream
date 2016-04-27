package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.LPipeline
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/26/2016.
  */
object LPipelineEncoder extends Encoder[LPipeline] {

  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], pipeline: LPipeline): Resource = {
    val resource = io.maker.createBNode()
    io.conn.add(resource, RDF.TYPE, Loam.pipeline)
    val pilesNode = RedFern.iterableEncoder[LPile](LPileEncoder).encode(io, pipeline.piles)
    io.conn.add(pilesNode, RDF.TYPE, Loam.set)
    io.conn.add(resource, Loam.hasPiles, pilesNode)
    val recipesNode = RedFern.iterableEncoder[LRecipe](LRecipeEncoder).encode(io, pipeline.recipes)
    io.conn.add(recipesNode, RDF.TYPE, Loam.set)
    io.conn.add(resource, Loam.hasRecipes, recipesNode)
    resource
  }
}
