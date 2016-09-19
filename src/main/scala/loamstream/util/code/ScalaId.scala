package loamstream.util.code

import javax.lang.model.SourceVersion

import scala.reflect.NameTransformer

/** A Scala identifier */
sealed trait ScalaId {

  /** The name itself without encoding or backticks */
  def name: String

  /** Short name as it appears in Scala code, with backticks if necessary */
  def inScala: String = ScalaId.withBackticksIfNeeded(name)

  /** Short name in JVM, encoded if necessary */
  def inJvm: String

  /** Full name as it appears in Scala code, with backticks if necessary */
  def inScalaFull: String

  /** Full name in JVM, encoded if necessary */
  def inJvmFull: String

  /** Some(parent) if it has a parent, None otherwise */
  def parentOpt: Option[ScalaId]

}

/** A Scala identifier */
object ScalaId {

  /** Returns true if name needs backticks in source code */
  def needsBackticks(name: String): Boolean = !SourceVersion.isName(name)

  /** Wraps into backticks if needed to appear in code */
  def withBackticksIfNeeded(name: String): String =
  if (needsBackticks(name)) {
    s"`$name`"
  } else {
    name
  }

}

/** A Scala identifier specifying a package */
object PackageId {
  /** Returns root package id */
  def apply(): RootPackageId.type = RootPackageId

  /** Returns top-level package id of that name */
  def apply(name: String): TopLevelPackageId = TopLevelPackageId(name)

  /** Returns sub-level package id of that parent and name */
  def apply(parent: ChildIdPackageId, name: String): SubLevelPackageId = SubLevelPackageId(parent, name)

  /** Returns sub-level package id composed of given names */
  def apply(part: String, parts: String*): SubLevelPackageId = PackageId(part +: parts).asInstanceOf[SubLevelPackageId]

  /** Returns sub-level package id composed of given names */
  def apply(parts: Seq[String]): PackageId =
    if (parts.isEmpty) {
      RootPackageId
    } else if (parts.size == 1) {
      TopLevelPackageId(parts.last)
    } else {
      SubLevelPackageId(PackageId(parts.dropRight(1)), parts.last)
    }
}

/** A package id */
sealed trait PackageId extends ScalaId {
  def inJvm: String = NameTransformer.encode(name)
}

/** An id other than root package, having a parent */
sealed trait ChildId extends ScalaId {
  /** Id of the parent */
  def parent: ScalaId

  def parentOpt: Some[ScalaId] = Some(parent)
}

/** The root package id */
object RootPackageId extends PackageId {
  override val name: String = "_root_"

  override val inScala: String = "_root_"

  override val inJvm: String = "_root_"

  override val inScalaFull: String = "_root_"

  override val inJvmFull: String = "_root_"

  override val parentOpt: None.type = None
}

/** A top level id, i.e. whose parent is the root package */
trait TopLevel extends ChildId {
  def parent: RootPackageId.type = RootPackageId

  override def inScalaFull: String = inScala

  override def inJvmFull: String = inJvm

}

/** A sub-level id, i.e. whose parent is not the root package */
trait SubLevel extends ChildId {
  override def inScalaFull: String = s"${parent.inScalaFull}.$inScala"

  override def inJvmFull: String = s"${parent.inJvmFull}.$inJvm"
}

/** A package id that is not the root package */
trait ChildIdPackageId extends PackageId with ChildId

/** A package id whose parent is the root package */
case class TopLevelPackageId(name: String) extends ChildIdPackageId with TopLevel

/** A package id whose parent is not the root package */
case class SubLevelPackageId(parent: PackageId, name: String) extends ChildIdPackageId with SubLevel

/** A class or trait id */
object TypeId {
  /** Returns a top level class or trait id of that name */
  def apply(name: String): TopLevelTypeId = TopLevelTypeId(name)

  /** Returns a sub-level class or trait id with that parent and name */
  def apply(parent: ChildIdPackageId, name: String): SubLevelTypeId = SubLevelTypeId(parent, name)

  /** Returns a sub-level class or trait id composed of these parts  */
  def apply(part: String, parts: String*): SubLevelTypeId = TypeId(part +: parts).asInstanceOf[SubLevelTypeId]

  /** Returns a sub-level class or trait id composed of these parts  */
  def apply(parts: Seq[String]): TypeId =
    if (parts.size == 1) {
      TopLevelTypeId(parts.last)
    } else {
      SubLevelTypeId(PackageId(parts.dropRight(1)), parts.last)
    }
}

/** A class or trait id */
trait TypeId extends ChildId {
  def inJvm: String = NameTransformer.encode(name)
}

/** An object id */
object ObjectId {
  /** Returns a top-level object id of that name */
  def apply(name: String): TopLevelObjectId = TopLevelObjectId(name)

  /** Returns a sub-level object id of that parent and name */
  def apply(parent: ChildIdPackageId, name: String): SubLevelObjectId = SubLevelObjectId(parent, name)

  /** Returns a sub-level object id composed of these names */
  def apply(part: String, parts: String*): SubLevelObjectId = ObjectId(part +: parts).asInstanceOf[SubLevelObjectId]

  /** Returns a sub-level object id composed of these names */
  def apply(parts: Seq[String]): ObjectId =
    if (parts.size == 1) {
      TopLevelObjectId(parts.last)
    } else {
      SubLevelObjectId(PackageId(parts.dropRight(1)), parts.last)
    }
}

/** An object id */
trait ObjectId extends ChildId {
  def inJvm: String = NameTransformer.encode(name) + "$"
}

/** A top-level class or trait id, i.e. whose parent is the root package */
case class TopLevelTypeId(name: String) extends TypeId with TopLevel

/** A sub-level class or trait id, i.e. whose parent is not the root package */
case class SubLevelTypeId(parent: ScalaId, name: String) extends TypeId with SubLevel

/** A top-level object id, i.e. whose parent is the root package */
case class TopLevelObjectId(name: String) extends ObjectId with TopLevel

/** A sub-level object id, i.e. whose parent is not the root package */
case class SubLevelObjectId(parent: ScalaId, name: String) extends ObjectId with SubLevel


