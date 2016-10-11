package loamstream.util.code

import javax.lang.model.SourceVersion

import scala.reflect.NameTransformer
import scala.reflect.runtime.universe.{Symbol, Type, TypeTag, rootMirror, typeOf}


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

  /** Full name in JVM, encoded if necessary
    *
    * Separator is dot after a package and dollar after a type. Objects already end with a dollar,
    * so no extra dollar */
  def inJvmFull: String

  /** Prefix to get the full name of a child id of this id in Scala */
  def childPrefixInScala: String

  /** Prefix to get the full name of a child id of this id in the JVM */
  def childPrefixInJvm: String

  /** Some(parent) if it has a parent, None otherwise */
  def parentOpt: Option[ScalaId]

  /** Create TypeId that is child of this ScalaId */
  def getType(name: String): TypeId = TypeId(this, name)

  /** Create ObjectId that is child of this ScalaId */
  def getObject(name: String): ObjectId = ObjectId(this, name)
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
  def apply(name: String): ChildPackageId = ChildPackageId(name)

  /** Returns sub-level package id of that parent and name */
  def apply(parent: ChildPackageId, name: String): ChildPackageId = ChildPackageId(parent, name)

  /** Returns sub-level package id composed of given names */
  def apply(part: String, parts: String*): ChildPackageId = PackageId(part +: parts).asInstanceOf[ChildPackageId]

  /** Returns sub-level package id composed of given names */
  def apply(parts: Seq[String]): PackageId =
  if (parts.isEmpty) {
    RootPackageId
  } else if (parts.size == 1) {
    ChildPackageId(parts.last)
  } else {
    ChildPackageId(PackageId(parts.dropRight(1)), parts.last)
  }

  /** Whether this symbol represents the root package */
  def isRootPackage(symbol: Symbol): Boolean = symbol.name == rootMirror.RootPackage.asModule.moduleClass.name

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
  def getPackage(name: String): ChildPackageId
}

/** An id other than root package, having a parent */
sealed trait ChildId extends ScalaId {
  /** Id of the parent */
  def parent: ScalaId

  def parentOpt: Some[ScalaId] = Some(parent)

  override def childPrefixInScala: String = s"$inScalaFull."

  override def inScalaFull: String = s"${parent.childPrefixInScala}$inScala"

  override def inJvmFull: String = s"${parent.childPrefixInJvm}$inJvm"
}

/** The root package id */
object RootPackageId extends PackageId with ScalaId {
  override val name: String = "_root_"

  override val inScala: String = "_root_"

  override val inJvm: String = "_root_"

  override val inScalaFull: String = "_root_"

  override val inJvmFull: String = "_root_"

  override val parentOpt: None.type = None

  /** Create TopLevelPackageId that is child of this ScalaId */
  override def getPackage(name: String): ChildPackageId = ChildPackageId(name)

  override def childPrefixInScala: String = ""

  override def childPrefixInJvm: String = ""
}

/** A package id that is not the root package */
object ChildPackageId {
  def apply(name: String): ChildPackageId = ChildPackageId(RootPackageId, name)
}

/** A package id that is not the root package */
case class ChildPackageId(parent: PackageId, name: String) extends PackageId with ChildId {
  /** Create SubLevelPackageId that is child of this ChildIdPackageId */
  override def getPackage(name: String): ChildPackageId = ChildPackageId(this, name)

  override def childPrefixInJvm: String = s"$inJvmFull."
}

/** A class or trait id */
object TypeId {
  /** Returns a top level class or trait id of that name */
  def apply(name: String): TypeId = TypeId(RootPackageId, name)

  /** Returns a sub-level class or trait id composed of these parts  */
  def apply(part: String, parts: String*): TypeId = TypeId(part +: parts)

  /** Returns a sub-level class or trait id composed of these parts  */
  def apply(parts: Seq[String]): TypeId =
  if (parts.size == 1) {
    TypeId(parts.last)
  } else {
    TypeId(PackageId(parts.dropRight(1)), parts.last)
  }
}

/** A class or trait id */
case class TypeId(parent: ScalaId, name: String) extends ChildId {
  def inJvm: String = NameTransformer.encode(name)

  override def childPrefixInJvm: String = s"$inJvmFull$$"
}

/** An object id */
object ObjectId {
  /** Returns a top-level object id of that name */
  def apply(name: String): ObjectId = ObjectId(RootPackageId, name)

  /** Returns a sub-level object id composed of these names */
  def apply(part: String, parts: String*): ObjectId = ObjectId(part +: parts)

  /** Returns a sub-level object id composed of these names */
  def apply(parts: Seq[String]): ObjectId =
  if (parts.size == 1) {
    ObjectId(parts.last)
  } else {
    ObjectId(PackageId(parts.dropRight(1)), parts.last)
  }
}

/** An object id */
case class ObjectId(parent: ScalaId, name: String) extends ChildId with ScalaId {
  def inJvm: String = NameTransformer.encode(name) + "$"

  override def childPrefixInJvm: String = s"$inJvmFull"
}

