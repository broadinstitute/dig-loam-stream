package loamstream.util.code

import scala.reflect.NameTransformer

/** A Scala identifier */
sealed trait ScalaId {

  def name: String

  def inScala: String = ScalaId.withBackticksIfNeeded(name)

  def inJvm: String

  def inScalaFull: String

  def inJvmFull: String

  def parentOpt: Option[ScalaId]

}

/** A Scala identifier */
object ScalaId {

  /** Returns true if name needs backticks in source code */
  def needsBackticks(name: String): Boolean = name != NameTransformer.encode(name)

  /** Wraps into backticks if needed to appear in code */
  def withBackticksIfNeeded(name: String): String =
  if (needsBackticks(name)) {
    s"`$name`"
  } else {
    name
  }

}

sealed trait PackageId extends ScalaId {
  def inJvm: String = NameTransformer.encode(name)
}

sealed trait ChildId extends ScalaId {
  def parent: ScalaId

  def parentOpt: Some[ScalaId] = Some(parent)
}

object RootPackageId extends PackageId {
  override val name: String = "_root_"

  override val inScala: String = "_root_"

  override val inJvm: String = "_root_"

  override val inScalaFull: String = "_root_"

  override val inJvmFull: String = "_root_"

  override val parentOpt: None.type = None
}

trait TopLevel extends ChildId {
  def parent: RootPackageId.type = RootPackageId

  override def inScalaFull: String = inScala

  override def inJvmFull: String = inJvm

}

trait SubLevel extends ChildId {
  override def inScalaFull: String = s"${parent.inScalaFull}.$inScala"

  override def inJvmFull: String = s"${parent.inJvmFull}.$inJvm"
}

trait ChildIdPackageId extends PackageId with ChildId

case class TopLevelPackageId(name: String) extends ChildIdPackageId with TopLevel

case class SubLevelPackageId(parent: PackageId, name: String) extends ChildIdPackageId with SubLevel

trait TypeId extends ChildId {
  def inJvm: String = NameTransformer.encode(name)
}

trait ObjectId extends ChildId {
  def inJvm: String = NameTransformer.encode(name) + "$"
}

case class TopLevelTypeId(name: String) extends TypeId with TopLevel

case class SubLevelTypeId(parent: ScalaId, name: String) extends TypeId with SubLevel

case class TopLevelObjectId(name: String) extends ObjectId with TopLevel

case class SubLevelObjectId(parent: ScalaId, name: String) extends ObjectId with SubLevel


