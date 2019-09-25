package loamstream

import org.scalatest.FunSuite

import loamstream.compiler.LoamEngine
import loamstream.conf.CompilationConfig
import loamstream.conf.ExecutionConfig
import loamstream.conf.LoamConfig
import loamstream.db.slick.DbDescriptor
import loamstream.db.slick.SlickLoamDao
import loamstream.loam.LoamSyntax
import loamstream.model.execute.DbBackedJobFilter
import loamstream.model.execute.HashingStrategy
import loamstream.model.execute.RxExecuter
import loamstream.util.Files


/**
 * @author clint
 * Sep 19, 2019
 */
final class LotsOfJobsDontCrashTheDbTest extends FunSuite {
  ignore("Lots of jobs, lots of concurrent DB access") {
    val numBranches = 350
        
    val jobsPerbranch = 10
    
    IntegrationTestHelpers.withWorkDirUnderTarget(Some(getClass.getSimpleName)) { workDir =>
      val graph = IntegrationTestHelpers.makeGraph() { implicit scriptContext =>
        import LoamSyntax._
        
        val input = workDir / "start.txt"

        Files.writeTo(input)("AAA")
        
        val beginningStore = store(input).asInput
        
        for(b <- 1 to numBranches) {
          def numberedOutput(i: Int): Store = store(workDir / s"${b}-out-${i}.txt")
          
          def tagName(i: Int): String = s"${b}-${i}"
          
          val out1 = numberedOutput(1)
          
          val z: Seq[(Tool, Store)] = {
            List(cmd"cp $beginningStore $out1".in(beginningStore).out(out1).tag(tagName(1)) -> out1)
          }
          
          (2 to jobsPerbranch).foldLeft(z) { (acc, i) =>
            val (prevTool, prevToolOutput) = acc.head
            
            val nextOutput = numberedOutput(i)
            
            val nextTool = cmd"cp $prevToolOutput $nextOutput".in(prevToolOutput).out(nextOutput).tag(tagName(i))
            
            (nextTool -> nextOutput) +: acc
          }
        }
      }
      
      assert(graph.tools.size === (numBranches * jobsPerbranch))
      
      val executable = LoamEngine.toExecutable(graph)
      
      assert(executable.jobNodes.size === numBranches)
      
      val dbDescriptor = DbDescriptor.onDiskHsqldbAt(workDir, getClass.getSimpleName)
      
      val dao = new SlickLoamDao(dbDescriptor)
      
      try {
        dao.createTables()
        
        val jobFilter = new DbBackedJobFilter(dao, HashingStrategy.HashOutputs)
        
        val executer = RxExecuter.defaultWith(newJobFilter = jobFilter)
        
        val results = executer.execute(executable)
      } finally {
        dao.dropTables()
      }
    }
  }
}
