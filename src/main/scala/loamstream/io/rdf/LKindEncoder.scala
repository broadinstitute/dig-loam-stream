package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.kinds.{LAnyKind, LKind, LNoKind, LSpecificKind}
import org.openrdf.model.vocabulary.{RDF, RDFS}
import org.openrdf.model.{Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/26/2016.
  */
object LKindEncoder extends Encoder[LKind] {
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], kind: LKind): Resource = {
    kind match {
      case LAnyKind => Loam.anyKind
      case LNoKind => Loam.noKind
      case LSpecificKind(specifics, supers) =>
        val resource = io.maker.createBNode()
        io.conn.add(resource, RDF.TYPE, Loam.specificKind)
        io.conn.add(resource, Loam.hasSpecifics, LValueEncoder.encode(io, specifics))
        supers.foreach({ superKind => io.conn.add(resource, RDFS.SUBCLASSOF, encode(io, superKind)) })
        resource
    }
  }
}
