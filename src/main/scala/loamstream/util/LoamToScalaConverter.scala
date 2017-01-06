package loamstream.util

import java.nio.file.{Paths, Files => JFiles}
import loamstream.loam.LoamScript
import java.nio.file.Path
import scalariform.formatter.ScalaFormatter.format

/** 
 *  @author oliver
 *  @author clint
 *  Jan 6, 2017
 *  
 *  Resolves Loam scripts into corresponding Scala files.
 *   
 */
object LoamToScalaConverter extends App with Loggable {

  import scala.collection.JavaConverters._
  
  def listLoamFiles(rootDir: Path, recursive: Boolean = true): Seq[Path] = {
    val loamExtension = ".loam"
    
    def isLoamFile(p: Path): Boolean = p.toString.endsWith(loamExtension)
    
    def isDir(p: Path): Boolean = p.toFile.isDirectory
    
    require(isDir(rootDir))
    
    def list(dir: Path, pred: Path => Boolean): Seq[Path] = {
      JFiles.list(rootDir).iterator.asScala.filter(_ != null).filter(pred).toIndexedSeq
    }
    
    def loams(dir: Path): Seq[Path] = list(dir, isLoamFile)
    
    def dirs(dir: Path): Seq[Path] = list(dir, isDir)
    
    loams(rootDir) ++ {
      if(!recursive) { Nil }  
      else {
        dirs(rootDir).flatMap(d => listLoamFiles(d, recursive))
      } 
    }
  }
  
  def convert(rootDir: Path, outputDir: Path, recursive: Boolean = true): Unit = {
    
    info(s"Looking at $rootDir.")
    
    val loamFiles = listLoamFiles(rootDir, recursive) 
    
    info(s"Found ${StringUtils.soMany(loamFiles.size, "Loam file")}, now loading.")
    
    val scriptShots = loamFiles.map(LoamScript.read)
    val scripts = Shot.sequence(scriptShots).get
    
    JFiles.createDirectories(outputDir)
    
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
    
    val contextOwnerFile = outputDir.resolve(s"$contextOwnerName.scala")
    
    info(s"Now writing Loam project owner file $contextOwnerFile")
    
    Files.writeTo(contextOwnerFile)(format(contextOwnerCode))
    
    info("Now writing Scala files derived from Loam scripts.")
    
    for(script <- scripts) {
      val scalaFile = outputDir.resolve(script.scalaFileName)
      
      val scalaCode = format(script.asScalaCode(contextValId))
      
      Files.writeTo(scalaFile)(scalaCode)
    }
    
    info("Done")
  }
  
  convert(Paths.get("src/main/loam/qc"), Paths.get("src/test/scala/loamstream/loam/scripts/qc"), recursive = false)
}
