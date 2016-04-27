package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.piles.LPile
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/26/2016.
  */
object LPileEncoder extends Encoder[LPile] {
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], pile: LPile): IRI = {
    val iri = LIdIris.toIri(pile)
    io.conn.add(iri, RDF.TYPE, Loam.pile)
    io.conn.add(iri, Loam.hasSig, LSigEncoder.encode(io, pile.spec.sig))
    io.conn.add(iri, RDF.TYPE, LKindEncoder.encode(io, pile.spec.kind))
    iri
  }
}
