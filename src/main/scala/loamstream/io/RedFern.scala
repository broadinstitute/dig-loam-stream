package loamstream.io

import java.util

import loamstream.util.shot.{Miss, Shot, Shots}
import org.openrdf.model.impl.LinkedHashModel
import org.openrdf.model.util.RDFCollections
import org.openrdf.model.vocabulary.XMLSchema
import org.openrdf.model.{IRI, Literal, Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection
import org.openrdf.repository.util.Connections

import scala.collection.JavaConverters.{asJavaIterableConverter, iterableAsScalaIterableConverter}
import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 4/12/2016.
  */
object RedFern {
  def getNew(implicit conn: RepositoryConnection): RedFern = RedFern(conn)

  type Encoder[T] = LIO.Encoder[RepositoryConnection, Value, ValueFactory, T]
  type Decoder[T] = LIO.Decoder[RepositoryConnection, Value, ValueFactory, T]

  case class LiteralEncoder[T](toLiteral: (ValueFactory, T) => Literal) extends Encoder[T] {
    override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], thing: T): Value =
      toLiteral(io.maker, thing)
  }

  case class LiteralDecoder[T](expectedDatatype: IRI, fromLiteral: Literal => T) extends Decoder[T] {
    override def decode(io: LIO[RepositoryConnection, Value, ValueFactory], value: Value): Shot[T] =
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

  implicit def iterableEncoder[E](implicit elementEncoder: Encoder[E]): Encoder[Iterable[E]] =
    new Encoder[Iterable[E]] {
      override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], iterable: Iterable[E]): Resource = {
        val head = io.maker.createBNode()
        val elementValues = iterable.map(elementEncoder.encode(io, _))
        val listModel = RDFCollections.asRDF(elementValues.asJava, head, new LinkedHashModel())
        io.conn.add(listModel)
        head
      }
    }

  implicit def iterableDecoder[E](implicit elementDecoder: Decoder[E]): Decoder[Iterable[E]] = {
    new Decoder[Iterable[E]] {
      override def decode(io: LIO[RepositoryConnection, Value, ValueFactory], ref: Value): Shot[Iterable[E]] = {
        ref match {
          case resource: Resource =>
            val rdfList = Connections.getRDFCollection(io.conn, resource, new LinkedHashModel())
            val values = RDFCollections.asValues(rdfList, resource, new util.ArrayList[Value])
            Shots.unpack(values.asScala.map(elementDecoder.decode(io, _)))
          case _ => Miss(s"Need resource to decode iterable, but got '$ref'")
        }
      }
    }
  }

  implicit def setDecoder[E](implicit elementDecoder: Decoder[E]): Decoder[Set[E]] = iterableDecoder[E].map(_.toSet)

  implicit def seqDecoder[E](implicit elementDecoder: Decoder[E]): Decoder[Seq[E]] = iterableDecoder[E].map(_.toSeq)

  implicit def tuple2Encoder[T1, T2](implicit encoder1: Encoder[T1], encoder2: Encoder[T2]): Encoder[(T1, T2)] = {
    new Encoder[(T1, T2)] {
      override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], tuple: (T1, T2)): Value = {
        val head = io.maker.createBNode()
        implicit val conn = io.conn
        conn.add(head, RdfContainers.m1, encoder1.encode(io, tuple._1))
        conn.add(head, RdfContainers.m2, encoder2.encode(io, tuple._2))
        head
      }
    }
  }

  implicit def tuple2Decoder[T1, T2](implicit decoder1: Decoder[T1], decoder2: Decoder[T2]): Decoder[(T1, T2)] = {
    new Decoder[(T1, T2)] {
      override def decode(io: LIO[RepositoryConnection, Value, ValueFactory], ref: Value): Shot[(T1, T2)] = {
        ref match {
          case resource: Resource =>
            implicit val conn = io.conn
            val m1Shot = RdfQueries.findUniqueObject(resource, RdfContainers.m1).flatMap(decoder1.decode(io, _))
            val m2Shot = RdfQueries.findUniqueObject(resource, RdfContainers.m2).flatMap(decoder2.decode(io, _))
            (m1Shot and m2Shot).get
          case _ => Miss(s"Need resource to decode Tuple2, but got '$ref'")
        }
      }
    }
  }
}

case class RedFern(conn: RepositoryConnection) extends LIO[RepositoryConnection, Value, ValueFactory] {
  val maker = conn.getValueFactory

}

