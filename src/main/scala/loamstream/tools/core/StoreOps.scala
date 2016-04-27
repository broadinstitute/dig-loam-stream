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
  
  final case class UnarySig(input: Store, output: Store)
  final case class BinarySig(inputs: (Store, Store), output: Store)
  
  final implicit class UnaryStoreOps(val lhs: Store) extends AnyVal {
    def ~>(rhs: Store): UnarySig = UnarySig(lhs, rhs)
  }
  
  final implicit class BinaryStoreOps(val lhs: (Store, Store)) extends AnyVal {
    def ~>(rhs: Store): BinarySig = BinarySig(lhs, rhs)
  }
}