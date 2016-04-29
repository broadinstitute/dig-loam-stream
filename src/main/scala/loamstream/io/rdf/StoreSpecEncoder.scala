package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.StoreSpec
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/29/2016.
  */
object StoreSpecEncoder extends Encoder[StoreSpec] {
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], storeSpec: StoreSpec): Resource = {
    val resource = io.maker.createBNode()
    io.conn.add(resource, RDF.TYPE, Loam.storeSpec)
    io.conn.add(resource, Loam.hasSig, LSigEncoder.encode(io, storeSpec.sig))
    io.conn.add(resource, RDF.TYPE, LKindEncoder.encode(io, storeSpec.kind))
    resource
  }
}
