package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.{LPipeline, Store, Tool}
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
    val storesNode = RedFern.iterableEncoder[Store](StoreEncoder).encode(io, pipeline.stores)
    io.conn.add(storesNode, RDF.TYPE, Loam.set)
    io.conn.add(resource, Loam.hasPiles, storesNode)
    val toolsNode = RedFern.iterableEncoder[Tool](ToolEncoder).encode(io, pipeline.tools)
    io.conn.add(toolsNode, RDF.TYPE, Loam.set)
    io.conn.add(resource, Loam.hasRecipes, toolsNode)
    resource
  }
}
