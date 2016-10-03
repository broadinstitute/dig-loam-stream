package loamstream.util.code

import javax.lang.model.SourceVersion

import scala.reflect.NameTransformer
import scala.reflect.runtime.universe.{Symbol, Type, TypeTag, typeOf}


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

  /** Create TypeId that is child of this ScalaId */
  def getType(name: String): TypeId

  /** Create ObjectId that is child of this ScalaId */
  def getObject(name: String): ObjectId
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

  /** Returns ScalaId of this type parameter */
  def from[T: TypeTag]: ScalaId = from(typeOf[T])

  /** Returns ScalaId of this type */
  def from(tpe: Type): ScalaId = from(tpe.typeSymbol)

  /** Returns ScalaId of this symbol */
  def from(symbol: Symbol): ScalaId = {
    if (PackageId.isRootPackage(symbol)) {
      RootPackageId
    } else if (symbol.isPackage) {
      val parentPackageId = PackageId.from(symbol.owner)
      parentPackageId.getPackage(symbol.name.toString)
    } else {
      val parentId = from(symbol.owner)
      if (symbol.isType) {
        parentId.getType(symbol.name.toString)
      } else {
        parentId.getObject(symbol.name.toString)
      }
    }
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

  /** Whether this symbol represents the root package */
  def isRootPackage(symbol: Symbol): Boolean = symbol.fullName.toString == symbol.owner.fullName.toString

  /** Returns PackageId of this Symbol */
  def from(symbol: Symbol): PackageId = {
    if (isRootPackage(symbol)) {
      RootPackageId
    } else {
      val parentId = from(symbol.owner)
      parentId.getPackage(symbol.name.toString)
    }
  }

}

/** A package id */
sealed trait PackageId extends ScalaId {
  def inJvm: String = NameTransformer.encode(name)

  /** Create ChildIdPackageId that is child of this ScalaId */
  def getPackage(name: String): ChildIdPackageId
}

/** An id other than root package, having a parent */
sealed trait ChildId extends ScalaId {
  /** Id of the parent */
  def parent: ScalaId

  def parentOpt: Some[ScalaId] = Some(parent)

  /** Create SubLeveTypeId that is child of this ScalaId */
  def getType(name: String): SubLevelTypeId = SubLevelTypeId(this, name)

  /** Create SubLeveObjectId that is child of this ScalaId */
  def getObject(name: String): SubLevelObjectId = SubLevelObjectId(this, name)
}

/** The root package id */
object RootPackageId extends PackageId {
  override val name: String = "_root_"

  override val inScala: String = "_root_"

  override val inJvm: String = "_root_"

  override val inScalaFull: String = "_root_"

  override val inJvmFull: String = "_root_"

  override val parentOpt: None.type = None

  /** Create TopLevelPackageId that is child of this ScalaId */
  override def getPackage(name: String): TopLevelPackageId = TopLevelPackageId(name)

  /** Create TypeId that is child of this ScalaId */
  override def getType(name: String): TopLevelTypeId = TopLevelTypeId(name)

  /** Create ObjectId that is child of this ScalaId */
  override def getObject(name: String): TopLevelObjectId = TopLevelObjectId(name)
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
trait ChildIdPackageId extends PackageId with ChildId {
  /** Create SubLevelPackageId that is child of this ChildIdPackageId */
  override def getPackage(name: String): SubLevelPackageId = SubLevelPackageId(this, name)

  /** Create SubLeveTypeId that is child of this ScalaId */
  override def getType(name: String): SubLevelTypeId = SubLevelTypeId(this, name)

  /** Create SubLeveObjectId that is child of this ScalaId */
  override def getObject(name: String): SubLevelObjectId = SubLevelObjectId(this, name)
}

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


