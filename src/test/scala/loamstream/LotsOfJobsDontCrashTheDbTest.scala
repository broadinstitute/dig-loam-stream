package loamstream

import org.scalatest.FunSuite
import loamstream.loam.LoamSyntax
import loamstream.util.Files
import loamstream.compiler.LoamEngine
import loamstream.model.execute.RxExecuter
import loamstream.model.execute.DbBackedJobFilter
import loamstream.db.slick.DbDescriptor
import loamstream.db.slick.SlickLoamDao
import loamstream.model.execute.HashingStrategy

/**
 * @author clint
 * Sep 19, 2019
 */
final class LotsOfJobsDontCrashTheDbTest extends FunSuite {
  test("Lots of jobs, lots of concurrent DB access") {
    val numBranches = 250
        
    val jobsPerbranch = 1
    
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val graph = TestHelpers.makeGraph { implicit scriptContext =>
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
      
      val dbDescriptor = DbDescriptor.onDiskAt(workDir, getClass.getSimpleName)
      
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
