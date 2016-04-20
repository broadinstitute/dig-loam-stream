package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.values.LType
import loamstream.model.values.LType._
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import org.openrdf.model.{Resource, Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/19/2016.
  */
object LTypeEncoder extends Encoder[LType[_]] {

  // scalastyle:off cyclomatic.complexity
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], tpe: LType[_]): Resource = {
    tpe match {
      case LBoolean => XMLSchema.BOOLEAN
      case LDouble => XMLSchema.DOUBLE
      case LFloat => XMLSchema.FLOAT
      case LLong => XMLSchema.LONG
      case LInt => XMLSchema.INT
      case LShort => XMLSchema.SHORT
      case LChar => Loam.char
      case LByte => XMLSchema.BYTE
      case LString => XMLSchema.STRING
      case LVariantId => Loam.variantId
      case LSampleId => Loam.sampleId
      case LGenotype => Loam.genotype
      case LSet(elementType) =>
        val setNode = io.maker.createBNode()
        io.conn.add(setNode, RDF.TYPE, Loam.set)
        io.conn.add(setNode, Loam.elementType, encode(io, elementType))
        setNode
      case LSeq(elementType) =>
        val seqNode = io.maker.createBNode()
        io.conn.add(seqNode, RDF.TYPE, Loam.seq)
        io.conn.add(seqNode, Loam.elementType, encode(io, elementType))
        seqNode
    }
  }

  // scalastyle:off cyclomatic.complexity
}
