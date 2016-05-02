package loamstream.io.rdf

import loamstream.model.values.LType
import loamstream.model.values.LType.{LBoolean, LByte, LChar, LDouble, LFloat, LGenotype, LInt, LLong, LSampleId,
LShort, LString, LVariantId}
import org.openrdf.model.IRI
import org.openrdf.model.vocabulary.XMLSchema

/**
  * LoamStream
  * Created by oliverr on 4/20/2016.
  */
object LTypeRdfDatatypeMapper {

  val typeToIri = Map[LType, IRI](LBoolean -> XMLSchema.BOOLEAN, LDouble -> XMLSchema.DOUBLE,
    LFloat -> XMLSchema.FLOAT, LLong -> XMLSchema.LONG, LInt -> XMLSchema.INT, LShort -> XMLSchema.SHORT,
    LChar -> Loam.char, LByte -> XMLSchema.BYTE, LString -> XMLSchema.STRING, LVariantId -> Loam.variantId,
    LSampleId -> Loam.sampleId, LGenotype -> Loam.genotype)

  val iriToType = typeToIri.collect({ case (key, value) => (value, key) })

}
