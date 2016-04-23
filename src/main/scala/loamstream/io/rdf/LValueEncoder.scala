package loamstream.io.rdf

import loamstream.io.LIO
import loamstream.io.rdf.RedFern.Encoder
import loamstream.model.values.LType.{LBoolean, LByte, LChar, LDouble, LFloat, LGenotype, LInt, LLong, LSampleId,
LShort, LString, LVariantId}
import loamstream.model.values.LValue
import org.openrdf.model.vocabulary.XMLSchema
import org.openrdf.model.{Value, ValueFactory}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/22/2016.
  */
object LValueEncoder extends Encoder[LValue] {


  // scalastyle:off cyclomatic.complexity
  override def encode(io: LIO[RepositoryConnection, Value, ValueFactory], lValue: LValue): Value = {
    lValue.tpe match {
      case LBoolean => io.maker.createLiteral(lValue.as[Boolean])
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
      case LGenotype => ???

    }
  }

  // scalastyle:off cyclomatic.complexity
}
