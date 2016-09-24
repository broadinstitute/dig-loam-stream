package loamstream.loam

import java.nio.file.{Files, Path}

import loamstream.compiler.LoamPredef
import loamstream.loam.LoamScript.{LoamScriptBox, scriptsPackage}
import loamstream.util.code.{ObjectId, PackageId, SourceUtils}
import loamstream.util.{Hit, Miss, PathEnrichments, Shot, StringUtils, ValueBox}

import scala.util.Try

/** A named Loam script */
object LoamScript {
  val scriptsPackage = PackageId("loamstream", "loam", "scripts")

  var scriptNameCounter: Int = 0
  val scriptNameCounterLock = new AnyRef

  val generatedNameBase = "LoamScript"

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

  def withGeneratedName(code: String): LoamScript = scriptNameCounterLock.synchronized({
    val name = s"$generatedNameBase${StringUtils.leftPadTo(scriptNameCounter.toString, "0", 3)}"
    scriptNameCounter += 1
    LoamScript(name, code)
  })

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

}

/** A named Loam script */
case class LoamScript(name: String, code: String) {

  def alphaNumHash = s"${name.##.toHexString}${code.##.toHexString}"

  def scalaId: ObjectId = ObjectId(scriptsPackage, name)

  def scriptContextName = s"context$alphaNumHash"

  def scalaFileName: String = s"$name.scala"

  def asScalaCode: String = {
    s"""
package ${LoamScript.scriptsPackage.inScalaFull}

import ${SourceUtils.fullTypeName[LoamPredef.type]}._
import ${SourceUtils.fullTypeName[LoamContext]}
import ${SourceUtils.fullTypeName[LoamGraph]}
import ${SourceUtils.fullTypeName[ValueBox[_]]}
import ${SourceUtils.fullTypeName[LoamScriptBox]}
import ${SourceUtils.fullTypeName[LoamCmdTool.type]}._
import ${SourceUtils.fullTypeName[PathEnrichments.type]}._
import java.nio.file._

object ${scalaId.inScala} extends ${SourceUtils.shortTypeName[LoamScriptBox]} {
  object LocalImplicits {
    implicit val $scriptContextName = LoamContext.empty
  }
import LocalImplicits._
def loamContext: LoamContext = $scriptContextName

${code.trim}

}
"""
  }


}
