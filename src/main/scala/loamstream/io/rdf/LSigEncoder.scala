package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.LSig
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/26/2016.
  */
object LSigEncoder extends Encoder[LSig] {
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], sig: LSig): Resource = {
    val resource = io.maker.createBNode()
    sig match {
      case LSig.Set(keyTypes) =>
        io.conn.add(resource, RDF.TYPE, Loam.set)
        io.conn.add(resource, Loam.keyTypes, LTypeEncoder.encode(io, keyTypes))
      case LSig.Map(keyTypes, vType) =>
        io.conn.add(resource, RDF.TYPE, Loam.set)
        io.conn.add(resource, Loam.keyTypes, LTypeEncoder.encode(io, keyTypes))
        io.conn.add(resource, Loam.valueType, LTypeEncoder.encode(io, vType))
    }
    resource
  }
}
