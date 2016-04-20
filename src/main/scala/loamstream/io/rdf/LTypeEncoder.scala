package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.values.LType
import loamstream.model.values.LType._
import org.openrdf.model.vocabulary.XMLSchema
import org.openrdf.model.{IRI, Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/19/2016.
  */
object LTypeEncoder extends Encoder[LType[_]] {

  case class AtomicEncoder[T](iri: IRI) extends Encoder[T] {
    override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], thing: T): IRI = iri
  }

  implicit val booleanEncoder = new AtomicEncoder[Boolean](XMLSchema.BOOLEAN)
  implicit val doubleEncoder = new AtomicEncoder[Double](XMLSchema.DOUBLE)
  implicit val floatEncoder = new AtomicEncoder[Float](XMLSchema.FLOAT)
  implicit val longEncoder = new AtomicEncoder[Long](XMLSchema.LONG)
  implicit val intEncoder = new AtomicEncoder[Int](XMLSchema.INT)
  implicit val shortEncoder = new AtomicEncoder[Short](XMLSchema.SHORT)
  implicit val byteEncoder = new AtomicEncoder[Byte](XMLSchema.BYTE)
  implicit val stringEncoder = new AtomicEncoder[String](XMLSchema.STRING)

  def encodeElementary[T](tpe: LType[T])(implicit encoder: AtomicEncoder[T]): IRI = encoder.iri

  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], tpe: LType[_]): Resource = {
    tpe match {
      case LBoolean => encodeElementary(LBoolean)
      case LDouble => encodeElementary(LDouble)
      case LFloat => encodeElementary(LFloat)
      case LLong => encodeElementary(LLong)
      case LInt => encodeElementary(LInt)
      case LShort => encodeElementary(LShort)
      case LByte => encodeElementary(LByte)
      case LString => encodeElementary(LString)
    }
  }
}
