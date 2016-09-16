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

object PackageId {
  def apply(): RootPackageId.type = RootPackageId

  def apply(name: String): TopLevelPackageId = TopLevelPackageId(name)

  def apply(parent: ChildIdPackageId, name: String): SubLevelPackageId = SubLevelPackageId(parent, name)

  def apply(part: String, parts: String*): SubLevelPackageId = PackageId(part +: parts).asInstanceOf[SubLevelPackageId]

  def apply(parts: Seq[String]): PackageId =
    if (parts.isEmpty) {
      RootPackageId
    } else if (parts.size == 1) {
      TopLevelPackageId(parts.last)
    } else {
      SubLevelPackageId(PackageId(parts.dropRight(1)), parts.last)
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

object TypeId {
  def apply(name: String): TopLevelTypeId = TopLevelTypeId(name)

  def apply(parent: ChildIdPackageId, name: String): SubLevelTypeId = SubLevelTypeId(parent, name)

  def apply(part: String, parts: String*): SubLevelTypeId = TypeId(part +: parts).asInstanceOf[SubLevelTypeId]

  def apply(parts: Seq[String]): TypeId =
    if (parts.size == 1) {
      TopLevelTypeId(parts.last)
    } else {
      SubLevelTypeId(PackageId(parts.dropRight(1)), parts.last)
    }
}

trait TypeId extends ChildId {
  def inJvm: String = NameTransformer.encode(name)
}

object ObjectId {
  def apply(name: String): TopLevelObjectId = TopLevelObjectId(name)

  def apply(parent: ChildIdPackageId, name: String): SubLevelObjectId = SubLevelObjectId(parent, name)

  def apply(part: String, parts: String*): SubLevelObjectId = ObjectId(part +: parts).asInstanceOf[SubLevelObjectId]

  def apply(parts: Seq[String]): ObjectId =
    if (parts.size == 1) {
      TopLevelObjectId(parts.last)
    } else {
      SubLevelObjectId(PackageId(parts.dropRight(1)), parts.last)
    }
}

trait ObjectId extends ChildId {
  def inJvm: String = NameTransformer.encode(name) + "$"
}

case class TopLevelTypeId(name: String) extends TypeId with TopLevel

case class SubLevelTypeId(parent: ScalaId, name: String) extends TypeId with SubLevel

case class TopLevelObjectId(name: String) extends ObjectId with TopLevel

case class SubLevelObjectId(parent: ScalaId, name: String) extends ObjectId with SubLevel


