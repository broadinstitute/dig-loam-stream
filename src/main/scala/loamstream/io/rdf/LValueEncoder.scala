package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.values.LType.LTuple.LTuple2
import loamstream.model.values.LType.{LBoolean, LByte, LChar, LDouble, LFloat, LInt, LLong, LMap, LSampleId, LSeq, LSet, LShort, LString, LTuple, LVariantId}
import loamstream.model.values.LValue
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import org.openrdf.model.{Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/22/2016.
  */
object LValueEncoder extends Encoder[LValue] {

  // scalastyle:off cyclomatic.complexity method.length
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], lValue: LValue): Value = {
    lValue.tpe match {
      case LBoolean => io.maker.createLiteral(lValue.valueAs[Boolean])
      case LDouble => io.maker.createLiteral(lValue.valueAs[Double])
      case LFloat => io.maker.createLiteral(lValue.valueAs[Float])
      case LLong => io.maker.createLiteral(lValue.valueAs[Long])
      case LInt => io.maker.createLiteral(lValue.valueAs[Int])
      case LShort => io.maker.createLiteral(lValue.valueAs[Short])
      case LChar => io.maker.createLiteral(lValue.valueAs[Char].toString, Loam.char)
      case LByte => io.maker.createLiteral(lValue.valueAs[Byte])
      case LString => io.maker.createLiteral(lValue.valueAs[String], XMLSchema.STRING)
      case LVariantId => io.maker.createLiteral(lValue.valueAs[String], Loam.variantId)
      case LSampleId => io.maker.createLiteral(lValue.valueAs[String], Loam.sampleId)
      case LSet(elementType) =>
        val setNode = RedFern.iterableEncoder[LValue](this).encode(io, lValue.valueAs[Set[_]].map(elementType.toValue))
        io.conn.add(setNode, RDF.TYPE, Loam.set)
        io.conn.add(setNode, Loam.elementType, LTypeEncoder.encode(io, elementType))
        setNode
      case LSeq(elementType) =>
        val seqNode = RedFern.iterableEncoder[LValue](this).encode(io, lValue.valueAs[Seq[_]].map(elementType.toValue))
        io.conn.add(seqNode, RDF.TYPE, Loam.seq)
        io.conn.add(seqNode, Loam.elementType, LTypeEncoder.encode(io, elementType))
        seqNode
      case LMap(keyType, valueType) =>
        val mapNode =
          RedFern.iterableEncoder[LValue](this).encode(io, lValue.valueAs[Map[_, _]].
            map(LTuple2(keyType, valueType).toValue(_)))
        io.conn.add(mapNode, RDF.TYPE, Loam.map)
        io.conn.add(mapNode, Loam.keyType, LTypeEncoder.encode(io, keyType))
        io.conn.add(mapNode, Loam.valueType, LTypeEncoder.encode(io, valueType))
        mapNode
      case tupleType: LTuple =>
        val tupleNode = io.maker.createBNode()
        val arity = tupleType.arity
        io.conn.add(tupleNode, RDF.TYPE, Loam.tuple(arity))
        val tupleValue = lValue.valueAs[Product]
        val valueNodes =
          tupleType.asSeq.zip(tupleValue.productIterator.toSeq).map({ case (lType, value) =>
            lType.toValue(value)
          }).map(encode(io, _))
        valueNodes.zipWithIndex.foreach({
          case (node, index) => io.conn.add(tupleNode, RdfContainers.membershipProperty(index)(io.conn), node)
        })
        tupleNode
      case _ =>
        val unknownNode = io.maker.createBNode()
        io.conn.add(unknownNode, RDF.TYPE, Loam.unknownType)
        io.conn.add(unknownNode, Loam.hasType, io.maker.createLiteral(lValue.tpe.toString, XMLSchema.STRING))
        io.conn.add(unknownNode, Loam.hasValue, io.maker.createLiteral(lValue.value.toString, XMLSchema.STRING))
        unknownNode
    }
  }

  // scalastyle:off cyclomatic.complexity method.length
}
