package loamstream.io

import loamstream.util.shot.{Miss, Shot}
import org.openrdf.model.vocabulary.XMLSchema
import org.openrdf.model.{IRI, Literal, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 4/12/2016.
  */
object RedFern {
  def getNew(implicit conn: RepositoryConnection): RedFern = RedFern(conn)

  type Encoder[T] = LIO.Encoder[Value, ValueFactory, T]
  type Decoder[T] = LIO.Decoder[Value, ValueFactory, T]

  case class LiteralEncoder[T](toLiteral: (ValueFactory, T) => Literal) extends Encoder[T] {
    override def encode(io: LIO[Value, ValueFactory], thing: T): Value = toLiteral(io.maker, thing)
  }

  case class LiteralDecoder[T](expectedDatatype: IRI, fromLiteral: Literal => T) extends Decoder[T] {
    override def read(io: LIO[Value, ValueFactory], value: Value): Shot[T] =
      value match {
        case literal: Literal =>
          val actualDatatype = literal.getDatatype
          if (actualDatatype == expectedDatatype) {
            Shot.fromTry(Try {
              fromLiteral(literal)
            })
          } else {
            Miss(s"Expected literal of data type '$expectedDatatype', but got '$actualDatatype'")
          }
        case _ => Miss(s"RDF value '$value' is not a literal")
      }

  }

  implicit val doubleEncoder = LiteralEncoder((maker: ValueFactory, double: Double) => maker.createLiteral(double))
  implicit val doubleDecoder = LiteralDecoder(XMLSchema.DOUBLE, _.doubleValue())
}

case class RedFern(conn: RepositoryConnection) extends LIO[Value, ValueFactory] {
  val maker = conn.getValueFactory

}

