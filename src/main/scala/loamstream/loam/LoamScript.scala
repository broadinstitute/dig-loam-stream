package loamstream.loam

import java.nio.file.Path

import loamstream.loam.LoamScript.LoamScriptBox
import loamstream.loam.LoamScript.scriptsPackage
import loamstream.util.DepositBox
import loamstream.util.Sequence
import loamstream.util.StringUtils
import loamstream.util.ValueBox
import loamstream.util.code.ObjectId
import loamstream.util.code.PackageId
import loamstream.util.code.ScalaId
import loamstream.util.code.SourceUtils
import scala.util.Try
import loamstream.util.Tries
import scala.util.Success
import loamstream.util.Files
import scala.util.Failure
import loamstream.loam.asscala.LoamFile

/** A named Loam script */
sealed trait LoamScript {
  def name: String
  //def code: String
  def subPackage: Option[PackageId]
  
  def pkg: PackageId
  
  /** Scala id of object corresponding to this Loam script */
  def scalaId: ObjectId = ObjectId(pkg, name)

  /** Name of Scala source file corresponding to this Loam script */
  def scalaFileName: String = s"$name.scala"
  
  def asScalaCode: String
}

object LoamScript {
  /** Package of Scala object corresponding to Loam scripts */
  val scriptsPackage = PackageId("loamstream", "loam", "scripts")

  /** Sequence of indices for auto-generated Loam script names */
  val generatedNameIndices: Sequence[Int] = new Sequence(0, 1)

  /** Base name for auto-generated Loam script names */
  val generatedNameBase = "loamScript"

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
      import scala.collection.JavaConverters._
      
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
      ScalaId.from[LoamScriptContext],
      ScalaId.from[LoamProjectContext],
      ScalaId.from[LoamProjectContext.type]
    )
  }
}

/** A named Loam .scala file */
final case class ScalaLoamScript(
    name: String,
    code: String, 
    pkg: PackageId = scriptsPackage, 
    subPackage: Option[PackageId] = None) extends LoamScript {
  
  override def asScalaCode: String = code
}

/** A named Loam script */
final case class LoamLoamScript(name: String, code: String, subPackage: Option[PackageId] = None) extends LoamScript {

  override def pkg: PackageId = scriptsPackage
  
  /** Convert to Scala code, provided code to create or obtain Loam project context */
  override def asScalaCode: String = {
    val packageForThisScript = subPackage match {
      case Some(subP) => LoamScript.scriptsPackage.getPackage(subP.inScala)
      case None => LoamScript.scriptsPackage
    }
    
    s"""package ${packageForThisScript.inScalaFull}

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
