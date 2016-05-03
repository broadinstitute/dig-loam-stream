package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Decoder
import loamstream.model.values.LType._
import loamstream.model.values.LValue
import loamstream.util.shot.Shots
import loamstream.util.{Hit, Miss, Shot, TupleUtil}
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import org.openrdf.model.{Literal, Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 4/26/16.
  */
object LValueDecoder extends Decoder[LValue] {
  // scalastyle:off cyclomatic.complexity method.length
  override def decode(io: LIO[RepositoryConnection, Value, ValueFactory], rdfNode: Value): Shot[LValue] = {
    rdfNode match {
      case literal: Literal =>
        val dataType = literal.getDatatype
        dataType match {
          case XMLSchema.BOOLEAN => Hit(LBoolean(literal.booleanValue()))
          case XMLSchema.DOUBLE => Hit(LDouble(literal.doubleValue()))
          case XMLSchema.FLOAT => Hit(LFloat(literal.floatValue()))
          case XMLSchema.LONG => Hit(LLong(literal.longValue()))
          case XMLSchema.INT => Hit(LInt(literal.intValue()))
          case XMLSchema.SHORT => Hit(LShort(literal.shortValue()))
          case Loam.char => Hit(LChar(literal.intValue().toChar))
          case XMLSchema.BYTE => Hit(LByte(literal.byteValue()))
          case XMLSchema.STRING => Hit(LString(literal.stringValue()))
          case Loam.variantId => Hit(LVariantId(literal.stringValue()))
          case Loam.sampleId => Hit(LSampleId(literal.stringValue()))
          case _ => Miss(s"Don't know how to decode literal with data type '$dataType'.")
        }
      case resource: Resource =>
        RdfQueries.findUniqueObject(resource, RDF.TYPE)(io.conn).flatMap({
          case rdfType: Resource =>
            rdfType match {
              case Loam.set =>
                val elementTypeShot = RdfQueries.findUniqueObject(resource, Loam.elementType)(io.conn).
                  flatMap(LTypeDecoder.decode(io, _)).flatMap(_.asEncodeable)
                val typeShot = elementTypeShot.map(LSet(_))
                val setShot = RedFern.setDecoder[LValue](this).decode(io, resource).map(_.map(_.value))
                (setShot and typeShot) (LValue(_, _))
              case Loam.seq =>
                val elementTypeShot = RdfQueries.findUniqueObject(resource, Loam.elementType)(io.conn).
                  flatMap(LTypeDecoder.decode(io, _)).flatMap(_.asEncodeable)
                val typeShot = elementTypeShot.map(LSeq)
                val seqShot = RedFern.seqDecoder[LValue](this).decode(io, resource).map(_.map(_.value))
                (seqShot and typeShot) (LValue(_, _))
              case Loam.map =>
                val keyTypeShot = RdfQueries.findUniqueObject(resource, Loam.keyType)(io.conn).
                  flatMap(LTypeDecoder.decode(io, _)).flatMap(_.asEncodeable)
                val valueTypeShot = RdfQueries.findUniqueObject(resource, Loam.valueType)(io.conn).
                  flatMap(LTypeDecoder.decode(io, _)).flatMap(_.asEncodeable)
                val typeShot = (keyTypeShot and valueTypeShot) (LMap)
                val mapShot = RedFern.mapDecoder[LValue, LValue](this, this).decode(io, resource).
                  map(_.map({ case (key, value) => (key.value, value.value) }))
                (mapShot and typeShot) (LValue(_, _))
              case _ if Loam.isTupleType(rdfType) =>
                val arityShot = Loam.tupleTypeToArity(rdfType)
                val lValuesShot = arityShot.flatMap({ arity =>
                  Shots.unpack((1 to arity).map({ index =>
                    RdfQueries.findUniqueObject(resource, RdfContainers.membershipProperty(index)(io.conn))(io.conn).
                      flatMap(decode(io, _))
                  })).map(_.toSeq)
                })
                val tupleValueShot = lValuesShot.map(_.map(_.value)).flatMap(TupleUtil.seqToProduct)
                val typeShot = lValuesShot.map(_.map(_.tpe)).flatMap(LTuple.seqToTupleShot(_))
                (tupleValueShot and typeShot) (LValue(_, _))
              case _ => Miss(s"Don't know how to decode instance of type '$rdfType'.")
            }
          case literal: Literal => Miss(s"Need resource as RDF type, but got Literal '$literal'.")
        })
    }
  }

  // scalastyle:on cyclomatic.complexity method.length
}
