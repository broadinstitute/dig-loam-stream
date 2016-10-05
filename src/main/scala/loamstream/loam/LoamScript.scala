package loamstream.loam

import java.nio.file.{Files, Path}

import loamstream.compiler.LoamPredef
import loamstream.loam.LoamScript.{LoamScriptBox, scriptsPackage}
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
    nameFromFilePath(path).flatMap({ name =>
      Shot.fromTry(Try {
        val code = StringUtils.fromUtf8Bytes(Files.readAllBytes(path))
        LoamScript(name, code)
      })
    })
  }

  /** A wrapper type for Loam scripts */
  trait LoamScriptBox {
    /** LoamContext for tis script */
    def loamContext: LoamContext

    /** The graph of stores and tools defined by this Loam script */
    def graph: LoamGraph = loamContext.graphBox.value
  }

  /** ScalaIds required to be loaded  */
  val requiredScalaIds: Set[ScalaId] =
  Set(ScalaId.from[LoamPredef.type],
    ScalaId.from[LoamPredef.VCF],
    ScalaId.from[LoamPredef.TXT],
    ScalaId.from[LoamCmdTool.type],
    ScalaId.from[LoamCmdTool.StringContextWithCmd],
    ScalaId.from[PathEnrichments.type],
    ScalaId.from[PathEnrichments.PathHelpers],
    ScalaId.from[PathEnrichments.PathAttemptHelpers])

}

/** A named Loam script */
case class LoamScript(name: String, code: String) {

  def alphaNumHash = s"${name.##.toHexString}${code.##.toHexString}"

  def scalaId: ObjectId = ObjectId(scriptsPackage, name)

  def scriptContextName = s"context$alphaNumHash"

  def scalaFileName: String = s"$name.scala"

  def asScalaCode: String = asScalaCode("LoamContext.empty")

  def asScalaCode(graphBoxReceipt: DepositBox.Receipt): String =
    asScalaCode(s"LoamContext.fromDepositedGraphBox(${graphBoxReceipt.asScalaCode})")

  def asScalaCode(loamContextInitCode: String): String = {
    s"""
package ${LoamScript.scriptsPackage.inScalaFull}

import ${ScalaId.from[LoamPredef.type].inScalaFull}._
import ${ScalaId.from[LoamContext].inScalaFull}
import ${ScalaId.from[LoamGraph].inScalaFull}
import ${ScalaId.from[ValueBox[_]].inScalaFull}
import ${ScalaId.from[LoamScriptBox].inScalaFull}
import ${ScalaId.from[LoamCmdTool.type].inScalaFull}._
import ${ScalaId.from[PathEnrichments.type].inScalaFull}._
import ${ScalaId.from[DepositBox[_]].inScalaFull}
import java.nio.file._

object ${scalaId.inScala} extends ${SourceUtils.shortTypeName[LoamScriptBox]} {
  object LocalImplicits {
    implicit val $scriptContextName = $loamContextInitCode
  }
import LocalImplicits._
def loamContext: LoamContext = $scriptContextName

${code.trim}

}
"""
  }


}
