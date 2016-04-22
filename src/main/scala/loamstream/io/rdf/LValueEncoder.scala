package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.values.LValue
import org.openrdf.model.{Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/22/2016.
  */
object LValueEncoder extends Encoder[LValue] {
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], lValue: LValue): Value = ???
}
