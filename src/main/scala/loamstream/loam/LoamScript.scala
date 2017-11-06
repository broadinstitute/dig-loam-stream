package loamstream.loam

import java.nio.file.Path

import loamstream.compiler.LoamPredef
import loamstream.loam.LoamScript.{LoamScriptBox, scriptsPackage}
import loamstream.loam.ops.StoreType
import loamstream.util._
import loamstream.util.code.{ObjectId, PackageId, ScalaId, SourceUtils}
import loamstream.googlecloud.GoogleSupport
import loamstream.googlecloud.GoogleSupport

/** A named Loam script */
object LoamScript {
  /** Package of Scala object corresponding to Loam scripts */
  val scriptsPackage = PackageId("loamstream", "loam", "scripts")

  /** Sequence of indices for auto-generated Loam script names */
  val generatedNameIndices: Sequence[Int] = new Sequence(0, 1)

  /** Base name for auto-generated Loam script names */
  val generatedNameBase = "loamScript"

  /** File suffix for Loam scripts - .loam */
  val fileSuffix = ".loam"

  /** File suffix for Scala source code - .scala */
  val scalaFileSuffix = ".scala"

  /** Extracts Loam script name from file Path with suffix .loam, removing suffix. */
  def nameFromFilePath(path: Path): Shot[String] = {
    val fileName = path.getFileName.toString
    if (fileName.endsWith(fileSuffix)) {
      Hit(fileName.dropRight(fileSuffix.length))
    } else {
      Miss(s"Missing suffix $fileSuffix")
    }
  }
  
  def nameAndEnclosingDirFromFilePath(path: Path, rootDir: Path): Shot[(String, Option[Path])] = {
    val fileName = path.getFileName.toString
    
    if (fileName.endsWith(fileSuffix)) {
      //NB: the result of .getParent might be null
      val relativeEnclosingDir = Option(rootDir.relativize(path).getParent)
      
      Hit(fileName.dropRight(fileSuffix.length) -> relativeEnclosingDir)
    } else {
      Miss(s"Missing suffix $fileSuffix")
    }
  }

  /** Converts Loam code fragment into Loam script, adding a auto-generated name */
  def withGeneratedName(code: String): LoamScript = {
    val index = generatedNameIndices.next()
    val name = s"$generatedNameBase${StringUtils.leftPadTo(index.toString, "0", 3)}"
    LoamScript(name, code, None)
  }

  /** Tries to read Loam script from file, deriving script name from file path. */
  def read(path: Path): Shot[LoamScript] = {
    import loamstream.util.Files.readFromAsUtf8
    
    for {
      name <- nameFromFilePath(path)
      code <- Shot(readFromAsUtf8(path))
    } yield {
      LoamScript(name, code, None)
    }
  }
  
  def read(path: Path, rootDir: Path): Shot[LoamScript] = {
    nameAndEnclosingDirFromFilePath(path, rootDir).flatMap { case (name, enclosingDirOpt) =>
      Shot {
        val code = loamstream.util.Files.readFromAsUtf8(path)
        
        import scala.collection.JavaConverters._
        
        val packageIdOpt = for {
          enclosingDir <- enclosingDirOpt
          packageParts = enclosingDir.iterator.asScala.toIndexedSeq.map(_.toString)
        } yield PackageId(packageParts)
        
        LoamScript(name, code, packageIdOpt)
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
    Set(ScalaId.from[LoamPredef.type],
      ScalaId.from[StoreType.VCF],
      ScalaId.from[StoreType.TXT],
      ScalaId.from[LoamCmdTool.type],
      ScalaId.from[LoamCmdTool.StringContextWithCmd],
      ScalaId.from[PathEnrichments.type],
      ScalaId.from[PathEnrichments.PathHelpers],
      ScalaId.from[PathEnrichments.PathAttemptHelpers],
      ScalaId.from[LoamScriptContext],
      ScalaId.from[LoamProjectContext],
      ScalaId.from[LoamProjectContext.type]
    )
  }
}

/** A named Loam script */
final case class LoamScript(name: String, code: String, subPackage: Option[PackageId] = None) {

  /** Scala id of object corresponding to this Loam script */
  def scalaId: ObjectId = ObjectId(scriptsPackage, name)

  /** Name of Scala source file corresponding to this Loam script */
  def scalaFileName: String = s"$name.scala"

  /** Convert to Scala code with own local Loam project context - only use for single Loam script */
  def asScalaCode: String = asScalaCode("new LoamScriptContext(LoamProjectContext.empty)")

  /** Convert to Scala code with Loam project context deposited in DepositBox */
  def asScalaCode(projectContextReceipt: DepositBox.Receipt): String = {
    asScalaCode(s"LoamScriptContext.fromDepositedProjectContext(${projectContextReceipt.asScalaCode})")
  }

  /** Convert to Scala code with Loam project context available via regular Scala reference */
  def asScalaCode(projectContextId: ScalaId): String = {
    asScalaCode(s"new LoamScriptContext(${projectContextId.inScalaFull})")
  }

  /** Convert to Scala code, provided code to create or obtain Loam project context */
  def asScalaCode(loamScriptContextCode: String): String = {
    val packageForThisScript = subPackage match {
      case Some(subP) => LoamScript.scriptsPackage.getPackage(subP.inScala)
      case None => LoamScript.scriptsPackage
    }
    
    s"""package ${packageForThisScript.inScalaFull}

import ${ScalaId.from[LoamPredef.type].inScalaFull}._
import ${ScalaId.from[LoamProjectContext].inScalaFull}
import ${ScalaId.from[LoamGraph].inScalaFull}
import ${ScalaId.from[ValueBox[_]].inScalaFull}
import ${ScalaId.from[LoamScriptBox].inScalaFull}
import ${ScalaId.from[LoamCmdTool.type].inScalaFull}._
import ${ScalaId.from[PathEnrichments.type].inScalaFull}._
import ${ScalaId.from[UriEnrichments.type].inScalaFull}._
import ${ScalaId.from[DepositBox[_]].inScalaFull}
import ${ScalaId.from[LoamProjectContext].inScalaFull}
import ${ScalaId.from[LoamScriptContext].inScalaFull}
import ${ScalaId.from[StoreType].inScalaFull}._
import ${ScalaId.from[GoogleSupport.type].inScalaFull}._
import java.nio.file._
import java.net.URI

// scalastyle:off object.name

object ${scalaId.inScala} extends ${SourceUtils.shortTypeName[LoamScriptBox]} {
object LocalImplicits {
  // scalastyle:off line.size.limit
  implicit val scriptContext : ${ScalaId.from[LoamScriptContext].inScala} = $loamScriptContextCode
  implicit val projectContext : ${ScalaId.from[LoamProjectContext].inScala} = scriptContext.projectContext
  // scalastyle:on line.size.limit
}
import LocalImplicits.{scriptContext => scriptContextImplicit, projectContext => projectContextImplicit }
override def scriptContext: LoamScriptContext = LocalImplicits.scriptContext
override def projectContext: LoamProjectContext = LocalImplicits.projectContext

//  = = =  Loam code below here  = = =

${code.trim}

//  = = =  Loam code above here  = = =
}
// scalastyle:on object.name
"""
  }

}
