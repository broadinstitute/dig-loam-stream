package loamstream.model.kinds

/**
 * @author clint
 * date: May 12, 2016
 */
final case class CompositeKind(components: Seq[LKind]) extends LKind {
  //TODO: Does this make sense?
  override def <:<(oKind: LKind): Boolean = components.forall(_ <:< oKind)

  //TODO: Does this make sense?
  override def >:>(oKind: LKind): Boolean = components.forall(_ >:> oKind)
}