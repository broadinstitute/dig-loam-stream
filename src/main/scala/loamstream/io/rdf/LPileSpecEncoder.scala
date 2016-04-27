package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.piles.LPileSpec
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/26/2016.
  */
object LPileSpecEncoder extends Encoder[LPileSpec] {
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], pileSpec: LPileSpec): Resource = {
    val resource = io.maker.createBNode()
    io.conn.add(resource, RDF.TYPE, Loam.pile)
    io.conn.add(resource, Loam.hasSig, LSigEncoder.encode(io, pileSpec.sig))
    io.conn.add(resource, RDF.TYPE, LKindEncoder.encode(io, pileSpec.kind))
    resource
  }
}
