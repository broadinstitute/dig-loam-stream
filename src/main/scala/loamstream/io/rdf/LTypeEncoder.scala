package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.values.LType
import loamstream.model.values.LType._
import org.openrdf.model.vocabulary.RDF
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
      case LBoolean | LDouble | LFloat | LLong | LInt | LShort | LChar | LByte | LString | LVariantId | LSampleId |
           LGenotype =>
        LTypeRdfDatatypeMapper.typeToIri(tpe)
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
      case LMap(keyType, valueType) =>
        val mapNode = io.maker.createBNode()
        io.conn.add(mapNode, RDF.TYPE, Loam.map)
        io.conn.add(mapNode, Loam.keyType, encode(io, keyType))
        io.conn.add(mapNode, Loam.valueType, encode(io, valueType))
        mapNode
    }
  }

  // scalastyle:off cyclomatic.complexity
}
