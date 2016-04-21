package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Decoder
import loamstream.model.values.LType.LSet
import loamstream.model.values.LTypeAny
import loamstream.util.shot.{Hit, Miss, Shot}
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import org.openrdf.model.{IRI, Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/20/2016.
  */
// scalastyle:off cyclomatic.complexity

object LTypeDecoder extends Decoder[LTypeAny] {
  override def decode(io: LIO[RepositoryConnection, Value, ValueFactory], typeNode: Value): Shot[LTypeAny] = {
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
                      map(LSet(_))
                  case Loam.seq => ???
                  case Loam.map => ???
                  case _ =>
                    Loam.tupleTypeToArity(rdfType) match {
                      case Hit(arity) => ???
                      case miss: Miss => miss
                    }
                }
              case miss: Miss => miss
            }
        }
      case _ => Miss(s"Expected resource, but got '$typeNode'")
    }
  }

  // scalastyle:on cyclomatic.complexity

}
