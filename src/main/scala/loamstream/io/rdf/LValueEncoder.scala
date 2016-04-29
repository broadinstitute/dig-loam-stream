package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.values.LType.{LBoolean, LByte, LChar, LDouble, LFloat, LInt, LLong, LMap, LSampleId, LSeq,
LSet, LShort, LString, LTuple, LVariantId}
import loamstream.model.values.LValue
import loamstream.util.TupleUtil
import org.openrdf.model.vocabulary.{RDF, XMLSchema}
import org.openrdf.model.{Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

import scala.collection.mutable

/**
  * LoamStream
  * Created by oliverr on 4/22/2016.
  */
object LValueEncoder extends Encoder[LValue[_]] {

  // scalastyle:off cyclomatic.complexity
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], lValue: LValue[_]): Value = {
    lValue.tpe match {
      case LBoolean => io.maker.createLiteral(lValue.as(LBoolean).value)
      case LDouble => io.maker.createLiteral(lValue.as[Double])
      case LFloat => io.maker.createLiteral(lValue.as[Float])
      case LLong => io.maker.createLiteral(lValue.as[Long])
      case LInt => io.maker.createLiteral(lValue.as[Int])
      case LShort => io.maker.createLiteral(lValue.as[Short])
      case LChar => io.maker.createLiteral(lValue.as[Char].toString, Loam.char)
      case LByte => io.maker.createLiteral(lValue.as[Byte])
      case LString => io.maker.createLiteral(lValue.as[String], XMLSchema.STRING)
      case LVariantId => io.maker.createLiteral(lValue.as[String], Loam.variantId)
      case LSampleId => io.maker.createLiteral(lValue.as[String], Loam.sampleId)
      case LSet(elementType) =>
        val setNode = RedFern.iterableEncoder[LValue](this).encode(io, lValue.as[Set[_]].map(elementType.value))
        io.conn.add(setNode, RDF.TYPE, Loam.set)
        io.conn.add(setNode, Loam.elementType, LTypeEncoder.encode(io, elementType))
        setNode
      case LSeq(elementType) =>
        val seqNode = RedFern.iterableEncoder[LValue](this).encode(io, lValue.as[Seq[_]].map(elementType.value))
        io.conn.add(seqNode, RDF.TYPE, Loam.seq)
        io.conn.add(seqNode, Loam.elementType, LTypeEncoder.encode(io, elementType))
        seqNode
      case LMap(keyType, valueType) =>
        val mapNode =
          RedFern.iterableEncoder[LValue](this).encode(io, lValue.as[Map[_, _]].
            map(LTuple(keyType, valueType).value(_)))
        io.conn.add(mapNode, RDF.TYPE, Loam.map)
        io.conn.add(mapNode, Loam.keyType, LTypeEncoder.encode(io, keyType))
        io.conn.add(mapNode, Loam.valueType, LTypeEncoder.encode(io, valueType))
        mapNode
      case LTuple(types) =>
        val tupleNode = io.maker.createBNode()
        val tuple = TupleUtil.seqToProduct(lValue.as[Seq[Any]]).get
        val arity = tuple.productArity
        io.conn.add(tupleNode, RDF.TYPE, Loam.tuple(arity))
        val valueNodes =
          types.zip(tuple.productIterator.toSeq).map({ case (lType, value) => lType.value(value) }).map(encode(io, _))
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

  // scalastyle:off cyclomatic.complexity
}
