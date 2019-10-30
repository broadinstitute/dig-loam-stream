package loamstream

import java.nio.file.Path
import java.nio.file.Paths

import org.apache.commons.io.FileUtils

import loamstream.conf.CompilationConfig
import loamstream.conf.ExecutionConfig
import loamstream.conf.LoamConfig
import loamstream.db.slick.DbDescriptor
import loamstream.loam.LoamGraph
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScriptContext
import loamstream.util.Sequence
import loamstream.db.slick.SlickLoamDao
import scala.util.Try

/**
 * @author clint
 * Jul 27, 2017
 */
object IntegrationTestHelpers {
  private val sequence: Sequence[Int] = Sequence()
  
  def path(s: String): Path = Paths.get(s)
  
  def inMemoryHsql(discriminator: String): DbDescriptor = {
    DbDescriptor.inMemoryHsqldb(s"integrationtest-${discriminator}")
  }
  
  def withWorkDirUnderTarget[A](subDir: Option[String] = None)(body: Path => A): A = {
    val workDir = getWorkDirUnderTarget(subDir)
    
    try {
      body(workDir)
    } finally {
      FileUtils.deleteQuietly(workDir.toFile)
    }
  }
  
  def getWorkDirUnderTarget(subDir: Option[String] = None): Path = {
    val classNamePart = getClass.getSimpleName match {
      case cn if cn.endsWith("$") => cn.dropRight(1)
      case cn => cn
    }
    
    val subDirParts = s"${classNamePart}-${sequence.next()}" +: subDir.toSeq
    
    val result = Paths.get("target", subDirParts: _*).toAbsolutePath
    
    try { 
      result 
    } finally {
      result.toFile.mkdirs()
    }
  }
  
  def makeGraph(config: LoamConfig = minimalConfig)(loamCode: LoamScriptContext => Any): LoamGraph = {
    withScriptContext(config) { sc =>
      loamCode(sc)
      
      sc.projectContext.graph
    }
  }
  
  def withScriptContext[A](config: LoamConfig = minimalConfig)(f: LoamScriptContext => A): A = {
    f(new LoamScriptContext(LoamProjectContext.empty(config)))
  }
  
  val minimalConfig: LoamConfig = LoamConfig(
    ugerConfig = None,
    lsfConfig = None,
    googleConfig = None,
    hailConfig = None,
    pythonConfig = None,
    rConfig = None,
    executionConfig = ExecutionConfig.default,
    compilationConfig = CompilationConfig.default,
    drmSystem = None,
    awsConfig = None)
    
  def createTablesAndThen[A](dao: SlickLoamDao)(f: => A): A = {
    //NB: Use Try(...) to succinctly ignore failures
    Try(dao.dropTables()) 
      
    dao.createTables()
      
    f
  }
}
