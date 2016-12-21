package loamstream.loam

import java.nio.file.{Files, Path}

import loamstream.compiler.LoamPredef
import loamstream.loam.LoamScript.{LoamScriptBox, scriptsPackage}
import loamstream.loam.ops.StoreType
import loamstream.loam.ops.filters.StoreFieldFilter
import loamstream.loam.ops.mappers.TextStoreFieldExtractor
import loamstream.util._
import loamstream.util.code.{ObjectId, PackageId, ScalaId, SourceUtils}

import scala.util.Try

/** A named Loam script */
object LoamScript {
  val scriptsPackage = PackageId("loamstream", "loam", "scripts")

  val generatedNameIndices: Sequence[Int] = new Sequence(0, 1)

  val generatedNameBase = "loamScript"

  val fileSuffix = ".loam"

  val scalaFileSuffix = ".scala"

  def nameFromFilePath(path: Path): Shot[String] = {
    val fileName = path.getFileName.toString
    if (fileName.endsWith(fileSuffix)) {
      Hit(fileName.dropRight(fileSuffix.length))
    } else {
      Miss(s"Missing suffix $fileSuffix")
    }
  }

  def withGeneratedName(code: String): LoamScript = {
    val index = generatedNameIndices.next()
    val name = s"$generatedNameBase${StringUtils.leftPadTo(index.toString, "0", 3)}"
    LoamScript(name, code)
  }

  def read(path: Path): Shot[LoamScript] = {
    for {
      name <- nameFromFilePath(path)
      code <- Shot(StringUtils.fromUtf8Bytes(Files.readAllBytes(path)))
    } yield {
      LoamScript(name, code)
    }
  }

  /** A wrapper type for Loam scripts */
  trait LoamScriptBox {
    /** LoamScriptContext for this script */
    def scriptContext: LoamScriptContext

    /** LoamScriptContext for this project */
    def projectContext: LoamProjectContext

    /** The graph of stores and tools defined by this Loam script */
    def graph: LoamGraph = projectContext.graphBox.value
  }

  /** ScalaIds required to be loaded  */
  val requiredScalaIds: Set[ScalaId] =
  Set(ScalaId.from[LoamPredef.type],
    ScalaId.from[StoreType.VCF],
    ScalaId.from[StoreType.TXT],
    ScalaId.from[StoreType.BIM],
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

/** A named Loam script */
case class LoamScript(name: String, code: String) {

  def alphaNumHash = s"${name.##.toHexString}${code.##.toHexString}"

  def scalaId: ObjectId = ObjectId(scriptsPackage, name)

  def scalaFileName: String = s"$name.scala"

  def asScalaCode: String = asScalaCode("new LoamScriptContext(LoamProjectContext.empty)")

  def asScalaCode(projectContextReceipt: DepositBox.Receipt): String =
    asScalaCode(s"LoamScriptContext.fromDepositedProjectContext(${projectContextReceipt.asScalaCode})")

  def asScalaCode(loamContextInitCode: String): String = {
    s"""
package ${LoamScript.scriptsPackage.inScalaFull}

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
import ${ScalaId.from[StoreFieldFilter.type].inScalaFull}
import ${ScalaId.from[TextStoreFieldExtractor[_, _]].inScalaFull}
import java.nio.file._

object ${scalaId.inScala} extends ${SourceUtils.shortTypeName[LoamScriptBox]} {
  object LocalImplicits {
    implicit val scriptContext : ${ScalaId.from[LoamScriptContext].inScala} = $loamContextInitCode
    implicit val projectContext : ${ScalaId.from[LoamProjectContext].inScala} = scriptContext.projectContext
  }
import LocalImplicits.{scriptContext => scriptContextImplicit, projectContext => projectContextImplicit }
def scriptContext: LoamScriptContext = LocalImplicits.scriptContext
def projectContext: LoamProjectContext = LocalImplicits.projectContext

${code.trim}

}
"""
  }


}
