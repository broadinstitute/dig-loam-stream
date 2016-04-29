package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.Store
import org.openrdf.model.vocabulary.RDF
import org.openrdf.model.{IRI, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/29/2016.
  */
object StoreEncoder extends Encoder[Store] {
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], store: Store): IRI = {
    val iri = LIdIris.toIri(store)
    io.conn.add(iri, RDF.TYPE, Loam.store)
    io.conn.add(iri, Loam.hasSig, LSigEncoder.encode(io, store.spec.sig))
    io.conn.add(iri, RDF.TYPE, LKindEncoder.encode(io, store.spec.kind))
    iri
  }
}
