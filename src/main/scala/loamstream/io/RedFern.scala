package loamstream.io

import loamstream.util.shot.{Miss, Shot}
import org.openrdf.model.vocabulary.XMLSchema
import org.openrdf.model.{IRI, Literal, Value}
import org.openrdf.repository.RepositoryConnection

import scala.util.Try

/**
  * LoamStream
  * Created by oliverr on 4/12/2016.
  */
object RedFern {
  def getNew(implicit conn: RepositoryConnection): RedFern = RedFern(conn)

  type Encoder[T] = LIO.Encoder[Value, T]
  type Decoder[T] = LIO.Decoder[Value, T]

}

case class RedFern(conn: RepositoryConnection) extends LIO[Value] {
  val valueFactory = conn.getValueFactory

  def extractValue[T](value: Value, expectedDatatype: IRI, extractor: Literal => T): Shot[T] = {
    value match {
      case literal: Literal =>
        val actualDatatype = literal.getDatatype
        if (actualDatatype == expectedDatatype) {
          Shot.fromTry(Try {
            extractor(literal)
          })
        } else {
          Miss(s"Expected literal of data type '$expectedDatatype', but got '$actualDatatype'")
        }
      case _ => Miss(s"RDF value '$value' is not a literal")
    }
  }

  override def writeDouble(double: Double): Literal = valueFactory.createLiteral(double)

  override def readDouble(value: Value): Shot[Double] = extractValue(value, XMLSchema.DOUBLE, _.doubleValue())

}

