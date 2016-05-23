package loamstream.tools.core

import loamstream.model.Store

/**
 * @author clint
 * date: Apr 26, 2016
 * 
 * NB: Possibly too clever, but helps to reduce noise and see the
 * forest for the trees.
 */
object StoreOps {
  
  //TODO TEST!
  
  sealed trait ToolSig {
    def toNarySig: NarySig
  }
  
  final case class UnarySig(input: Store, output: Store) extends ToolSig {
    override def toNarySig: NarySig = NarySig(Seq(input), Seq(output))
  }
  
  final case class BinarySig(inputs: (Store, Store), output: Store) extends ToolSig {
    override def toNarySig: NarySig = NarySig(Seq(inputs._1, inputs._2), Seq(output))
  }
  
  final case class NarySig(inputs: Seq[Store], outputs: Seq[Store]) extends ToolSig {
    override def toNarySig: NarySig = this
  }
  
  final implicit class UnaryStoreOps(val lhs: Store) extends AnyVal {
    def ~>(rhs: Store): UnarySig = UnarySig(lhs, rhs)
  }
  
  final implicit class BinaryStoreOps(val lhs: (Store, Store)) extends AnyVal {
    def ~>(rhs: Store): BinarySig = BinarySig(lhs, rhs)
  }
  
  final implicit class NaryStoreOps(val lhs: Seq[Store]) extends AnyVal {
    def ~>(rhs: Store): NarySig = NarySig(lhs, Seq(rhs))
    
    def ~>(rhs: Seq[Store]): NarySig = NarySig(lhs, rhs)
  }
}