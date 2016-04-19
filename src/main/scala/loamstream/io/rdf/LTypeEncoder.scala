package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.values.LType
import loamstream.model.values.LType.LBoolean
import org.openrdf.model.vocabulary.XMLSchema
import org.openrdf.model.{IRI, Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/19/2016.
  */
object LTypeEncoder extends Encoder[LType[_]] {

  case class ElementaryEncoder[T](iri: IRI) extends Encoder[T] {
    override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], thing: T): IRI = iri
  }

  implicit val booleanEncoder = new ElementaryEncoder[LBoolean.type](XMLSchema.BOOLEAN)

  def encodeElementary[T](tpe: LType[T])(implicit encoder: ElementaryEncoder[T]): IRI = encoder.iri

  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], tpe: LType[_]): Resource = {
    tpe match {
      case LBoolean => encodeElementary(LBoolean)
    }
  }
}
