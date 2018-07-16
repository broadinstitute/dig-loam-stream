package loamstream.util

import java.nio.file.{Paths => JPaths}
import java.nio.file.{Files => JFiles}
import loamstream.loam.LoamScript
import java.nio.file.Path
import java.io.File
import loamstream.util.code.RootPackageId
import loamstream.util.code.ObjectId
import loamstream.util.code.PackageId
import org.scalafmt.Scalafmt
import scala.util.control.NonFatal

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

  def convert(rootDir: Path, outputDir: Path, recursive: Boolean = true): Seq[File] = {

    info(s"Making dir: $outputDir")
    
    JFiles.createDirectories(outputDir)
    
    info(s"Looking at $rootDir.")
    
    val loamInfos = locateAndParseLoamFiles(rootDir, recursive)

    val dirsWithLoamFiles = {
      (loamInfos.keySet.map(_.getParent) + rootDir).filter(containsLoamFiles).map(rootDir.relativize)
    }
    
    info(s"Now writing ${dirsWithLoamFiles.size} Loam project owner file(s)")
    
    //NB: Side-effecting map :(
    val contextOwnerFiles = dirsWithLoamFiles.map(subDir => makeProjectContextOwnerFile(outputDir, subDir))
    
    info("Now writing Scala files derived from Loam scripts.")
    
    val scalaFiles = for {
      (loamFile, info) <- loamInfos
    } yield {
      makeScalaFile(outputDir, loamFile, info)
    }
    
    val filesCreated = contextOwnerFiles.toSeq ++ scalaFiles.toSeq
    
    info("Done")
    
    filesCreated.map(_.toFile)
  }
  
  def main(args: Array[String]): Unit = {
    info(s"Running ${getClass.getSimpleName}.main with args ${args.map(s => s"'$s'").mkString(", ")}")
    
    val Array(inputDirString, outputDirString) = args

    val inputDir = JPaths.get(inputDirString)
    
    val outputDir = JPaths.get(outputDirString)
    
    convert(inputDir, outputDir, recursive = true)
  }
  
  private def formatAndWrite(scalaCode: String, fileName: Path): Unit = {
    val codeToWrite = try {
      Scalafmt.format(scalaCode).get
    } catch {
      case NonFatal(e) => { 
        warn(s"Forging onward after error formatting code destined for '$fileName': $e", e)
        
        scalaCode
      }
    }
    
    Files.writeTo(fileName)(codeToWrite)
  }
  
  private val loamExtension = ".loam"
    
  private def isLoamFile(p: Path): Boolean = p.toString.endsWith(loamExtension)
    
  private def isDir(p: Path): Boolean = p.toFile.isDirectory
    
  private def list(dir: Path, pred: Path => Boolean): Seq[Path] = {
    require(isDir(dir), s"'$dir' must be a directory")
    
    JFiles.list(dir).iterator.asScala.filter(pred).toIndexedSeq
  }
    
  private def dirs(dir: Path): Seq[Path] = list(dir, isDir)
  
  private[util] def listLoamFiles(rootDir: Path, recursive: Boolean = true): Seq[Path] = {
    val loams: Seq[Path] = list(rootDir, isLoamFile)
    
    loams ++ {
      if(!recursive) { Nil }  
      else {
        dirs(rootDir).flatMap(d => listLoamFiles(d, recursive))
      } 
    }
  }
  
  private[util] def locateDirs(rootDir: Path, recursive: Boolean): Set[Path] = {
    val directories = dirs(rootDir).toSet
    
    directories ++ {
      if(!recursive) { Set.empty }
      else {
        directories.flatMap(d => locateDirs(d, recursive))
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
  
  private[util] def makeProjectContextOwnerFile(outputDir: Path, subDir: Path): Path = {
    val subPackageParts: Seq[String] = pathParts(subDir)
      
    val (contextOwnerName, contextOwnerPackageId, contextValId, contextOwnerCode) = {
      makeProjectContextOwnerCode(subPackageParts)
    }
    
    val relativeScriptPackageDir = getRelativeScriptPackageDir(subPackageParts)
  
    val scriptPackageDir = outputDir.resolve(relativeScriptPackageDir)
    
    JFiles.createDirectories(scriptPackageDir)
  
    val contextOwnerFile = scriptPackageDir.resolve(s"$contextOwnerName.scala")
    
    formatAndWrite(contextOwnerCode, contextOwnerFile)
    
    info(s"Created $contextOwnerFile")
    
    contextOwnerFile
  }
  
  private[util] def makeProjectContextOwnerCode(subPackages: Seq[String]): (String, PackageId, ObjectId, String) = {
    val contextOwnerName = "LoamProjectContextOwner"
    
    val contextOwnerPackage: PackageId = {
      if(subPackages.isEmpty) { LoamScript.scriptsPackage }
      else {
        subPackages.foldLeft(LoamScript.scriptsPackage)(_.getPackage(_))
      }
    }
    
    val contextOwnerId = contextOwnerPackage.getObject(contextOwnerName)
    val contextValName = "loamProjectContext"
    val contextValId = contextOwnerId.getObject(contextValName)
    val contextOwnerCode = {
      s"""package ${contextOwnerId.parent.inScalaFull}
        |
        |import loamstream.loam.LoamProjectContext
        |
        |object ${contextOwnerId.name} {
        |  val $contextValName : LoamProjectContext = LoamProjectContext.empty(???) //TODO
        |}
      """.stripMargin
    }
    
    (contextOwnerName, contextOwnerPackage, contextValId, contextOwnerCode)
  }
  
  private[util] def getRelativeScriptPackageDir(subPackages: Seq[String]): Path = {
    val partsWithoutUnderscoreRoot = LoamScript.scriptsPackage.parts.filterNot(_ == RootPackageId.name)
    
    val partsWithSubPackages = partsWithoutUnderscoreRoot ++ subPackages
    
    //This pattern match is safe as long as `LoamScript.scriptsPackage` is not the root package
    val first +: rest = partsWithSubPackages
    
    Paths.newRelative(first, rest: _*)
  }
  
  private[util] def pathParts(p: Path): Seq[String] = {
    p.iterator.asScala.map(_.toString).filterNot(_ == ".").filterNot(_.isEmpty).toIndexedSeq
  }
  
  private[util] def makeScalaFile(outputDir: Path, loamFile: Path, loamInfo: LoamFileInfo): Path = {
    
    val LoamFileInfo(relativePathToLoam, script) = loamInfo
    
    //NB: getParent might return null
    val subDir = Option(relativePathToLoam.getParent)
    
    val subPackageParts = subDir.map(pathParts).getOrElse(Nil)
    
    val (_, _, contextValId: ObjectId, _) = makeProjectContextOwnerCode(subPackageParts)
    val relativeScriptPackageDir = getRelativeScriptPackageDir(subPackageParts)
    
    val scalaFileOutputDir = outputDir.resolve(relativeScriptPackageDir)
      
    JFiles.createDirectories(scalaFileOutputDir)
      
    val scalaFile = scalaFileOutputDir.resolve(script.scalaFileName)
      
    val scalaCode = script.asScalaCode(contextValId)
      
    formatAndWrite(scalaCode, scalaFile)
      
    info(s"Created $scalaFile")
    
    scalaFile
  }
  
  private[util] def containsLoamFiles(d: Path): Boolean = list(d, isLoamFile).nonEmpty
}
