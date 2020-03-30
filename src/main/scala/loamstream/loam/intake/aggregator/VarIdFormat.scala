package loamstream.loam.intake.aggregator

/**
 * @author clint
 * Feb 12, 2020
 */
object VarIdFormat {
  def isValid(stringRep: String): Boolean = all4.unapplySeq(stringRep) match {
    case Some(groups) => groups.sorted == Component.names
    case _ => false
  }
  
  private val all4 = "^.*?\\{(.+)\\}.*?\\{(.+)\\}.*?\\{(.+)\\}.*?\\{(.+)\\}.*$".r
  
  sealed abstract class Component(val name: String)
  
  object Component {
    case object Chrom extends Component("chrom")
    case object Pos extends Component("pos")
    case object Ref extends Component("ref")
    case object Alt extends Component("alt")
    
    val values: Set[Component] = Set(Chrom, Pos, Ref, Alt)
    
    val names: Seq[String] = values.toSeq.map(_.name).sorted
  }
}
