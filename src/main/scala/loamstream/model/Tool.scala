package loamstream.model

import java.nio.file.Path

/**
  * @author Clint
  * @author Oliver
  *         date: Apr 26, 2016
  */
trait Tool extends LId.Owner {
  def inputs: Map[LId, Store]

  def outputs: Map[LId, Store]
}

object Tool {

  object ParamNames {
    val input = LId.LNamedId("input")
    val output = LId.LNamedId("output")
  }

  import StoreOps._

  abstract class OneToOne(sig: UnarySig) extends Tool {

    override val inputs: Map[LId, Store] = Map(ParamNames.input -> sig.input)

    override val outputs: Map[LId, Store] = Map(ParamNames.output -> sig.output)
  }

  abstract class Nary(sig: NarySig) extends Tool {

    def this(binarySig: BinarySig) = this(binarySig.toNarySig)

    def this(unarySig: UnarySig) = this(unarySig.toNarySig)

    override val inputs: Map[LId, Store] = sig.inputs.map(i => i.id -> i).toMap //TODO: correct?

    override val outputs: Map[LId, Store] = sig.outputs.map(o => o.id -> o).toMap //TODO: correct?
  }

  abstract class Nullary(outputStore: Store) extends Tool {
    override def inputs: Map[LId, Store] = Map.empty

    override val outputs: Map[LId, Store] = Map(ParamNames.output -> outputStore)
  }

  abstract class CheckPreexisting(file: Path, spec: StoreSig) extends Nullary(FileStore(file, spec)) {
    override val id = LId.LNamedId(s"Check for $file")
  }

}