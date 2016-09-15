package loamstream.loam

import java.nio.file.{Files, Path}

import loamstream.loam.LoamScript.scriptsPackage
import loamstream.util.{Hit, Miss, Shot, StringUtils, TypeName}

import scala.util.Try

/** A named Loam script */
object LoamScript {
  val scriptsPackage = TypeName("loamstream", "loam", "scripts")

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
}

/** A named Loam script */
case class LoamScript(name: String, code: String) {

  def typeName: TypeName = scriptsPackage + name

}
