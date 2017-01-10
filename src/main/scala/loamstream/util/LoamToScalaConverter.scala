package loamstream.util

import java.nio.file.{Paths, Files => JFiles}
import loamstream.loam.LoamScript
import java.nio.file.Path
import scalariform.formatter.ScalaFormatter.format
import java.io.File
import loamstream.util.code.RootPackageId
import loamstream.util.code.ObjectId

/** 
 *  @author oliver
 *  @author clint
 *  Jan 6, 2017
 *  
 *  Resolves Loam scripts into corresponding Scala files.
 *   
 */
object LoamToScalaConverter extends Loggable {

  import scala.collection.JavaConverters._
  
  private[util] def listLoamFiles(rootDir: Path, recursive: Boolean = true): Seq[Path] = {
    val loamExtension = ".loam"
    
    def isLoamFile(p: Path): Boolean = p.toString.endsWith(loamExtension)
    
    def isDir(p: Path): Boolean = p.toFile.isDirectory
    
    require(isDir(rootDir))
    
    def list(dir: Path, pred: Path => Boolean): Seq[Path] = {
      JFiles.list(rootDir).iterator.asScala.filter(pred).toIndexedSeq
    }
    
    val loams: Seq[Path] = list(rootDir, isLoamFile)
    
    def dirs(dir: Path): Seq[Path] = list(dir, isDir)
    
    loams ++ {
      if(!recursive) { Nil }  
      else {
        dirs(rootDir).flatMap(d => listLoamFiles(d, recursive))
      } 
    }
  }
  
  private[util] final case class LoamFileInfo(relativePath: Path, script: LoamScript)
  
  private[util] def locateAndParseLoamFiles(rootDir: Path, recursive: Boolean): Map[Path, LoamFileInfo] = {
    val loamFiles = listLoamFiles(rootDir, recursive)
    
    info(s"Found ${StringUtils.soMany(loamFiles.size, "Loam file")}, now loading.")

    import Traversables.Implicits._
    
    loamFiles.mapTo { loam =>
      val relativePath = rootDir.relativize(loam)
      val script = LoamScript.read(loam, rootDir).get
      
      LoamFileInfo(relativePath, script)
    }
  }
  
  private[util] def makeProjectContextOwnerCode: (String, ObjectId, String) = {
    val contextOwnerName = "LoamProjectContextOwner"
    val contextOwnerId = LoamScript.scriptsPackage.getObject(contextOwnerName)
    val contextValName = "loamProjectContext"
    val contextValId = contextOwnerId.getObject(contextValName)
    val contextOwnerCode = {
      s"""package ${contextOwnerId.parent.inScalaFull}
        |
        |import loamstream.loam.LoamProjectContext
        |
        |object ${contextOwnerId.name} {
        |  val $contextValName : LoamProjectContext = LoamProjectContext.empty
        |}
      """.stripMargin
    }
    
    (contextOwnerName, contextValId, contextOwnerCode)
  }
  
  private[util] def getRelativeScriptPackageDir: Path = {
    val partsWithoutUnderscoreRoot = LoamScript.scriptsPackage.parts.filterNot(_ == RootPackageId.name)
    
    //This pattern match is safe as long as `LoamScript.scriptsPackage` is not the root package
    val first +: rest = partsWithoutUnderscoreRoot
    
    PathUtils.newRelative(first, rest: _*)
  }
  
  private[util] def makeScalaFile(outputDir: Path, loamFile: Path, loamInfo: LoamFileInfo): Unit = {
    
    val (_, contextValId: ObjectId, _) = makeProjectContextOwnerCode
    val relativeScriptPackageDir = getRelativeScriptPackageDir
    
    val LoamFileInfo(relativePathToLoam, script) = loamInfo
    
    val relativeOutputDir = Option(relativePathToLoam.getParent)
      
    val scriptPackageOutputDir = outputDir.resolve(relativeScriptPackageDir)
    
    val scalaFileOutputDir = relativeOutputDir match {
      case Some(oDir) => scriptPackageOutputDir.resolve(oDir)
      case None => scriptPackageOutputDir
    }
      
    JFiles.createDirectories(scalaFileOutputDir)
      
    val scalaFile = scalaFileOutputDir.resolve(script.scalaFileName)
      
    val scalaCode = format(script.asScalaCode(contextValId))
      
    Files.writeTo(scalaFile)(scalaCode)
      
    info(s"Created $scalaFile")
  }
  
  def convert(rootDir: Path, outputDir: Path, recursive: Boolean = true): Unit = {
    
    info(s"Looking at $rootDir.")
    
    val loamInfos = locateAndParseLoamFiles(rootDir, recursive)
    
    JFiles.createDirectories(outputDir)

    val (contextOwnerName, contextValId, contextOwnerCode) = makeProjectContextOwnerCode
    
    val relativeScriptPackageDir = getRelativeScriptPackageDir
    
    val scriptPackageDir = outputDir.resolve(relativeScriptPackageDir)
    
    JFiles.createDirectories(scriptPackageDir)
    
    val contextOwnerFile = scriptPackageDir.resolve(s"$contextOwnerName.scala")
    
    info(s"Now writing Loam project owner file $contextOwnerFile")
    
    Files.writeTo(contextOwnerFile)(format(contextOwnerCode))
    
    info("Now writing Scala files derived from Loam scripts.")
    
    for {
      (loamFile, info) <- loamInfos
    } {
      makeScalaFile(outputDir, loamFile, info)
    }
    
    info("Done")
  }
  
  def main(args: Array[String]): Unit = {
    //TODO: Don't hard-code output path
    convert(Paths.get("src/main/loam/"), Paths.get("target/scala-2.11/src_managed/main"), recursive = true)
  }
}
