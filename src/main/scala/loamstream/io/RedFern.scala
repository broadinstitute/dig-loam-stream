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
  implicit val floatEncoder = LiteralEncoder((maker: ValueFactory, float: Float) => maker.createLiteral(float))
  implicit val floatDecoder = LiteralDecoder(XMLSchema.FLOAT, _.floatValue())
  implicit val longEncoder = LiteralEncoder((maker: ValueFactory, long: Long) => maker.createLiteral(long))
  implicit val longDecoder = LiteralDecoder(XMLSchema.LONG, _.longValue())
  implicit val intEncoder = LiteralEncoder((maker: ValueFactory, int: Int) => maker.createLiteral(int))
  implicit val intDecoder = LiteralDecoder(XMLSchema.INT, _.intValue())
  implicit val shortEncoder = LiteralEncoder((maker: ValueFactory, short: Short) => maker.createLiteral(short))
  implicit val shortDecoder = LiteralDecoder(XMLSchema.SHORT, _.shortValue())
  implicit val byteEncoder = LiteralEncoder((maker: ValueFactory, byte: Byte) => maker.createLiteral(byte))
  implicit val byteDecoder = LiteralDecoder(XMLSchema.BYTE, _.byteValue())
  implicit val booleanEncoder =
    LiteralEncoder((maker: ValueFactory, boolean: Boolean) => maker.createLiteral(boolean))
  implicit val booleanDecoder = LiteralDecoder(XMLSchema.BOOLEAN, _.booleanValue())
  implicit val stringEncoder =
    LiteralEncoder((maker: ValueFactory, string: String) => maker.createLiteral(string, XMLSchema.STRING))
  implicit val stringDecoder = LiteralDecoder(XMLSchema.STRING, _.stringValue())
}

case class RedFern(conn: RepositoryConnection) extends LIO[Value, ValueFactory] {
  val maker = conn.getValueFactory

}

