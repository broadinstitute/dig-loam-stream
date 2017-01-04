package loamstream.apps

import java.nio.file.{Paths, Files => JFiles}

import loamstream.loam.LoamScript
import loamstream.util.{Files, Loggable, Shot, StringUtils}

import scala.collection.JavaConverters.asScalaIteratorConverter

/** App to resolve QC Loam scripts into corresponding Scala files */
object QcToScalaApp extends App with Loggable {

  val qcLoamDir = Paths.get("src", "main", "loam", "qc")
  info(s"Looking at $qcLoamDir.")
  val files = JFiles.list(qcLoamDir)
  val loamFiles = files.iterator().asScala.filter(_.toString.endsWith(".loam")).toSeq
  info(s"Found ${StringUtils.soMany(loamFiles.size, "Loam file")}, now loading.")
  val scriptShots = loamFiles.map(LoamScript.read)
  val scripts = Shot.sequence(scriptShots).get
  val qcScalaDir = Paths.get("src", "test", "scala", "loamstream", "loam", "scripts", "qc")
  JFiles.createDirectories(qcScalaDir)
  val contextOwnerName = "LoamProjectContextOwner"
  val contextOwnerId = LoamScript.scriptsPackage.getObject(contextOwnerName)
  val contextValName = "loamProjectContext"
  val contextValId = contextOwnerId.getObject(contextValName)
  val contextOwnerCode =
    s"""package ${contextOwnerId.parent.inScalaFull}
      |
      |import loamstream.loam.LoamProjectContext
      |
      |object ${contextOwnerId.name} {
      |  val $contextValName : LoamProjectContext = LoamProjectContext.empty
      |}
    """.stripMargin
  val contextOwnerFile = qcScalaDir.resolve(s"$contextOwnerName.scala")
  info(s"Now writing Loam project owner file $contextOwnerFile")
  Files.writeTo(contextOwnerFile)(contextOwnerCode)
  info("Now writing Scala files derived from Loam scripts.")
  for(script <- scripts) {
    val scalaFile = qcScalaDir.resolve(script.scalaFileName)
    val scalaCode = script.asScalaCode(contextValId)
    Files.writeTo(scalaFile)(scalaCode)
  }
  info("Done")
}
