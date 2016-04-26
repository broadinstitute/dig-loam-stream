package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.piles.LPile
import org.openrdf.model.{Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/26/2016.
  */
object LPileEncoder extends Encoder[LPile] {
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], thing: LPile): Resource = ???
}
