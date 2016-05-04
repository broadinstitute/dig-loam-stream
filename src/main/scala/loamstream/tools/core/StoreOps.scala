package loamstream.tools.core

import loamstream.model.StoreSpec

/**
 * @author clint
 * date: Apr 26, 2016
 * 
 * NB: Possibly too clever, but helps to reduce noise and see the
 * forest for the trees.
 */
object StoreOps {
  
  sealed trait ToolSig {
    def toNarySig: NarySig
  }
  
  final case class UnarySig(input: StoreSpec, output: StoreSpec) extends ToolSig {
    override def toNarySig: NarySig = NarySig(Seq(input), output)
  }
  
  final case class BinarySig(inputs: (StoreSpec, StoreSpec), output: StoreSpec) extends ToolSig {
    override def toNarySig: NarySig = NarySig(Seq(inputs._1, inputs._2), output)
  }
  
  final case class NarySig(inputs: Seq[StoreSpec], output: StoreSpec) extends ToolSig {
    override def toNarySig: NarySig = this
  }
  
  final implicit class UnaryStoreOps(val lhs: StoreSpec) extends AnyVal {
    def ~>(rhs: StoreSpec): UnarySig = UnarySig(lhs, rhs)
  }
  
  final implicit class BinaryStoreOps(val lhs: (StoreSpec, StoreSpec)) extends AnyVal {
    def ~>(rhs: StoreSpec): BinarySig = BinarySig(lhs, rhs)
  }
  
  final implicit class NaryStoreOps(val lhs: Seq[StoreSpec]) extends AnyVal {
    def ~>(rhs: StoreSpec): NarySig = NarySig(lhs, rhs)
  }
}