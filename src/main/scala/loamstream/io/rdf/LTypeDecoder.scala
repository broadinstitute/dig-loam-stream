package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Decoder
import loamstream.model.values.{LType, LTypeAny}
import loamstream.util.shot.{Hit, Miss, Shot}
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import org.openrdf.model.{IRI, Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/20/2016.
  */
object LTypeDecoder extends Decoder[LTypeAny] {
  override def decode(io: LIO[RepositoryConnection, Value, ValueFactory], typeNode: Value): Shot[LTypeAny] = {
    typeNode match {
      case resource: Resource =>
        resource match {
          case XMLSchema.BOOLEAN | XMLSchema.DOUBLE | XMLSchema.FLOAT | XMLSchema.LONG | XMLSchema.INT |
               XMLSchema.SHORT | Loam.char | XMLSchema.BYTE | XMLSchema.STRING | Loam.variantId | Loam.sampleId |
               Loam.genotype =>
            Hit(LTypeRdfDatatypeMapper.iriToType(resource.asInstanceOf[IRI]))
          case _ =>
            RdfQueries.findUniqueObject(resource, RDF.TYPE)(io.conn) match {
              case Hit(rdfType) =>
                ??? // TODO
              case miss: Miss => miss
            }
        }
      case _ => Miss(s"Expected resource, but got '$typeNode'")
    }
  }
}
