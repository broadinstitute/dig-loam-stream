package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Decoder
import loamstream.model.values.LType
import loamstream.model.values.LType.{LMap, LSeq, LSet, LTuple}
import loamstream.util.shot.Shots
import loamstream.util.{Hit, Miss, Shot}
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import org.openrdf.model.{IRI, Literal, Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/20/2016.
  */
// scalastyle:off cyclomatic.complexity

object LTypeDecoder extends Decoder[LType] {
  override def decode(io: LIO[RepositoryConnection, Value, ValueFactory], typeNode: Value): Shot[LType] = {
    typeNode match {
      case typeResource: Resource =>
        typeResource match {
          case XMLSchema.BOOLEAN | XMLSchema.DOUBLE | XMLSchema.FLOAT | XMLSchema.LONG | XMLSchema.INT |
               XMLSchema.SHORT | Loam.char | XMLSchema.BYTE | XMLSchema.STRING | Loam.variantId | Loam.sampleId |
               Loam.genotype =>
            Hit(LTypeRdfDatatypeMapper.iriToType(typeResource.asInstanceOf[IRI]))
          case _ =>
            RdfQueries.findUniqueObject(typeResource, RDF.TYPE)(io.conn) match {
              case Hit(rdfType) =>
                rdfType match {
                  case Loam.set =>
                    RdfQueries.findUniqueObject(typeResource, Loam.elementType)(io.conn).flatMap(decode(io, _)).
                      flatMap(_.asEncodeable).map(LSet(_))
                  case Loam.seq =>
                    RdfQueries.findUniqueObject(typeResource, Loam.elementType)(io.conn).flatMap(decode(io, _)).
                      flatMap(_.asEncodeable).map(LSeq(_))
                  case Loam.map =>
                    val keyTypeShot =
                      RdfQueries.findUniqueObject(typeResource, Loam.keyType)(io.conn).flatMap(decode(io, _)).
                        flatMap(_.asEncodeable)
                    val valueTypeShot =
                      RdfQueries.findUniqueObject(typeResource, Loam.valueType)(io.conn).flatMap(decode(io, _)).
                        flatMap(_.asEncodeable)
                    (keyTypeShot and valueTypeShot) (LMap)
                  case _ if Loam.isTupleType(rdfType) =>
                    Loam.tupleTypeToArity(rdfType) match {
                      case Hit(arity) =>
                        val typeShots = (1 to arity).map(i =>
                          RdfQueries.findUniqueObject(typeResource,
                            RdfContainers.membershipProperty(i)(io.conn))(io.conn)
                        ).map(_.flatMap(decode(io, _)).flatMap(_.asEncodeable))
                        Shots.unpack(typeShots).map(_.toSeq).map(LTuple(_))
                      case miss: Miss => miss
                    }
                  case literal: Literal => Miss(s"Expected RDF type to be a resource, but got literal '$literal'.")
                  case _ => Miss(s"Unrecognized type for an LType '$rdfType'.")
                }
              case miss: Miss => miss
            }
        }
      case _ => Miss(s"Expected resource, but got '$typeNode'")
    }
  }

  // scalastyle:on cyclomatic.complexity

}
