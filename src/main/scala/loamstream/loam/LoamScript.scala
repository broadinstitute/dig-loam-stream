package loamstream.loam

import java.nio.file.Path

import scala.util.Success
import scala.util.Try

import loamstream.compiler.LoamFile
import loamstream.loam.LoamScript.LoamScriptBox
import loamstream.loam.LoamScript.scriptsPackage
import loamstream.util.Files
import loamstream.util.Sequence
import loamstream.util.StringUtils
import loamstream.util.Tries
import loamstream.util.code.ObjectId
import loamstream.util.code.PackageId
import loamstream.util.code.RootPackageId
import loamstream.util.code.ScalaId
import loamstream.util.code.SourceUtils

/**
 * @author oliverr
 * ???, 2016
 * @author clint
 * May, 2020
 *  
 * A trait to represent a handle to Loam code, usually read from storage, either
 * as a .scala or a .loam file.  In the latter case, the contents of the .loam file
 * are wrapped in needed support code before compilation.
 */
sealed trait LoamScript {
  def name: String

  def subPackage: Option[PackageId]
  
  def pkg: PackageId = LoamScript.scriptsPackage
  
  protected def packageForThisScript: PackageId = subPackage match {
    case Some(subP) => pkg.getPackage(subP.inScala)
    case None => pkg
  }
  
  /** Scala id of object corresponding to this Loam script */
  def scalaId: ObjectId = ObjectId(packageForThisScript, name)

  /** Name of Scala source file corresponding to this Loam script */
  def scalaFileName: String = s"$name.scala"
  
  def asScalaCode: String
}

/** A named Loam .scala file */
final case class ScalaLoamScript(
    name: String,
    code: String, 
    subPackage: Option[PackageId] = None) extends LoamScript {
  
  override def asScalaCode: String = code
}

/** A named Loam script */
final case class LoamLoamScript(name: String, code: String, subPackage: Option[PackageId] = None) extends LoamScript {
  
  /** Convert to Scala code, provided code to create or obtain Loam project context */
  override def asScalaCode: String = {
    s"""
import ${ScalaId.from[LoamFile].inScalaFull}
import java.nio.file._

// scalastyle:off object.name

object ${scalaId.inScala} extends ${SourceUtils.shortTypeName[LoamFile]} {
//  = = =  Loam code below here  = = =

${code.trim}

//  = = =  Loam code above here  = = =
}
// scalastyle:on object.name
"""
  }
}

object LoamScript {
  /** Package of Scala object corresponding to Loam scripts */
  
  val scriptsPackage: PackageId = RootPackageId

  /** Sequence of indices for auto-generated Loam script names */
  val generatedNameIndices: Sequence[Int] = new Sequence(0, 1)

  /** Base name for auto-generated Loam script names */
  val generatedNameBase: String = "loamScript"

  /** File suffix for Loam scripts - .loam */
  private val fileSuffix: String = ScriptType.Loam.suffix

  /** File suffix for Scala source code - .scala */
  val scalaFileSuffix: String = ScriptType.Scala.suffix

  /** Extracts Loam script name from file Path with suffix .loam, removing suffix. */
  def nameFromFilePath(path: Path): Try[String] = {
    Files.tryFile(path).flatMap {
      case LoamFile.Name(name) => Success(name)
      case ScalaFile.Name(name) => Success(name)
      case _ => {
        val msg = s"Expected suffix to be one of $fileSuffix or $scalaFileSuffix, but filename was '${path.toString}'"
        
        Tries.failure(msg)
      }
    }
  }
  
  def scriptTypeFromFilePath(path: Path): Try[ScriptType] = {
    Files.tryFile(path).flatMap {
      case LoamFile.Type(scriptType) => Success(scriptType)
      case ScalaFile.Type(scriptType) => Success(scriptType)
      case _ => {
        val msg = s"Expected suffix to be one of $fileSuffix or $scalaFileSuffix, but filename was '${path.toString}'"
        
        Tries.failure(msg)
      } 
    }
  }
    
  private[LoamScript] object LoamFile extends ScriptDataExtractors(ScriptType.Loam)
  
  private[LoamScript] object ScalaFile extends ScriptDataExtractors(ScriptType.Scala)

  private[LoamScript] abstract class ScriptDataExtractors(scriptType: ScriptType) {
    object Name {
      def unapply(path: Path): Option[String] = {
        val fileName = path.getFileName.toString
       
        import scriptType.suffix
        
        if (fileName.endsWith(suffix)) { Some(fileName.dropRight(suffix.length)) } 
        else { None }
      }
    }
    
    object Type {
      def unapply(path: Path): Option[ScriptType] = {
        val fileName = path.getFileName.toString
        
        if (fileName.endsWith(scriptType.suffix)) { Some(scriptType) } 
        else { None }
      }
    }
  }
  
  def nameAndEnclosingDirFromFilePath(path: Path, rootDir: Path): Try[(String, Option[Path])] = {
    val fileName = path.getFileName.toString
    
    if (fileName.endsWith(fileSuffix)) {
      //NB: the result of .getParent might be null
      val relativeEnclosingDir = Option(rootDir.relativize(path).getParent)
      
      Success(fileName.dropRight(fileSuffix.length) -> relativeEnclosingDir)
    } else {
      Tries.failure(s"Missing suffix $fileSuffix")
    }
  }

  /** Converts Loam code fragment into Loam script, adding a auto-generated name */
  def withGeneratedName(code: String): LoamScript = {
    val index = generatedNameIndices.next()
    val name = s"$generatedNameBase${StringUtils.leftPadTo(index.toString, "0", 3)}"
    LoamLoamScript(name, code, None)
  }

  /** Tries to read Loam script from file, deriving script name from file path. */
  def read(path: Path): Try[LoamScript] = {
    import loamstream.util.Files.readFromAsUtf8
    
    for {
      p <- Files.tryFile(path)
      name <- nameFromFilePath(p)
      tpe <- scriptTypeFromFilePath(p)
      code <- Try(readFromAsUtf8(p))
    } yield {
      tpe match {
        case ScriptType.Loam => LoamLoamScript(name, code)
        case ScriptType.Scala => ScalaLoamScript(name, code)
      }
    }
  }
  
  def read(path: Path, rootDir: Path): Try[LoamScript] = {
    nameAndEnclosingDirFromFilePath(path, rootDir).flatMap { case (name, enclosingDirOpt) =>
      import scala.jdk.CollectionConverters._
      
      for {
        p <- Files.tryFile(path)
        code = loamstream.util.Files.readFromAsUtf8(path)
        packageIdOpt = enclosingDirOpt.map { enclosingDir =>
          val packageParts = enclosingDir.iterator.asScala.toIndexedSeq.map(_.toString)
          
          PackageId(packageParts)
        }
        tpe <- scriptTypeFromFilePath(path)
      } yield {
        tpe match {
          case ScriptType.Loam => LoamLoamScript(name = name, code = code, subPackage = packageIdOpt)
          case ScriptType.Scala => ScalaLoamScript(name = name, code = code, subPackage = packageIdOpt)
        }
      }
    }
  }

  /** A wrapper type for Loam scripts */
  trait LoamScriptBox {
    /** LoamScriptContext for this script */
    def scriptContext: LoamScriptContext

    /** LoamScriptContext for this project */
    def projectContext: LoamProjectContext

    /** The graph of stores and tools defined by this Loam script */
    def graph: LoamGraph = projectContext.graph
  }

  /** ScalaIds required to be loaded  */
  val requiredScalaIds: Set[ScalaId] = {
    Set(
      ScalaId.from[LoamSyntax.type],
      ScalaId.from[LoamFile],
      ScalaId.from[LoamScriptContext],
      ScalaId.from[LoamProjectContext],
      ScalaId.from[LoamProjectContext.type]
    )
  }
}
