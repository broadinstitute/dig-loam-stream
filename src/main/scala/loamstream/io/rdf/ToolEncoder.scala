package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.{Store, StoreSpec, Tool}
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/29/2016.
  */
object ToolEncoder extends Encoder[Tool] {
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], tool: Tool): IRI = {
    val iri = LIdIris.toIri(tool)
    io.conn.add(iri, RDF.TYPE, Loam.tool)
    io.conn.add(iri, RDF.TYPE, LKindEncoder.encode(io, tool.spec.kind))
    val inputSpecsNode = RedFern.iterableEncoder[StoreSpec](StoreSpecEncoder).encode(io, tool.spec.inputs)
    io.conn.add(inputSpecsNode, RDF.TYPE, Loam.seq)
    io.conn.add(iri, Loam.requiresInputs, inputSpecsNode)
    io.conn.add(iri, Loam.providesOutput, StoreSpecEncoder.encode(io, tool.spec.output))
    val inputsNode = RedFern.iterableEncoder[Store](StoreEncoder).encode(io, tool.inputs)
    io.conn.add(inputsNode, RDF.TYPE, Loam.seq)
    io.conn.add(iri, Loam.hasInputs, inputsNode)
    io.conn.add(iri, Loam.hasOutput, StoreEncoder.encode(io, tool.output))
    iri
  }
}
