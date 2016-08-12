package loamstream.model.execute

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import loamstream.model.jobs.LJob.Result
import loamstream.util.Hash
import loamstream.util.Maps
import loamstream.util.Shot
import loamstream.util.ValueBox
import loamstream.db.LoamDao
import loamstream.model.jobs.LJob
import loamstream.model.jobs.Output
import scala.annotation.migration
import loamstream.db.OutputRow
import loamstream.util.TimeEnrichments
import loamstream.util.Traversables

/**
 * @author clint
 * date: Aug 4, 2016
 * 
 * NB: This class contains a naive, first-pass sketch of an executor that takes job outputs' hashes into account.
 * It doesn't make use of any stored information (yet) and is for illustrative purposes only.
 */
final class NaiveHashingExecuter(dao: LoamDao)(implicit context: ExecutionContext) extends LExecuter {
  
  override def execute(executable: LExecutable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Shot[Result]] = {
    def toShotMap(m: Map[LJob, Result]): Map[LJob, Shot[Result]] = m.mapValues(Shot(_))
    
    val futureResults = runAndMerge(executable.jobs).map(toShotMap)
    
    //TODO
    Await.result(futureResults, timeout)
  }
  
  private[this] lazy val outputs: ValueBox[Map[Output, OutputRow]] = {
    //TODO: All of them?  
    val map: Map[Output, OutputRow] = dao.allRows.map { row => 
      Output.CachedOutput(row.path, row.hash, row.lastModified) -> row
    }.toMap
    
    ValueBox(map)
  }
  
  private def isHashed(output: Output): Boolean = outputs.value.contains(output)
  
  private def notHashed(output: Output): Boolean = !isHashed(output)
      
  private def hasDifferentHash(output: Output): Boolean = {
    outputs.value.get(output) match {
      case Some(outputRow) => outputRow.hash != output.hash
      case None => true
    }
  }
  
  private def isOlder(output: Output): Boolean = {
    import TimeEnrichments._
    
    outputs.value.get(output) match {
      case Some(outputRow) => output.lastModified < outputRow.lastModified
      case None => false
    }
  }
  
  private def shouldRun(dep: LJob): Boolean = {
    /*println(s"shouldRun(): Considering $dep")
    println(s"shouldRun(): Outputs: ${if(dep.outputs.isEmpty) "<none>" else ""}")
    dep.outputs.foreach { o =>
      println(s"shouldRun():   output: $o")
    }*/
    
    def needsToBeRun(output: Output): Boolean = {
      /*println(s"shouldRun().needsToBeRun(): Considering: $output")
      
      println(s"shouldRun().needsToBeRun(): isMissing: ${output.isMissing}")
      println(s"shouldRun().needsToBeRun(): isOlder(output): ${isOlder(output)}")
      println(s"shouldRun().needsToBeRun(): notHashed: ${notHashed(output)}")
      println(s"shouldRun().needsToBeRun(): hasDifferentHash: ${hasDifferentHash(output)}")*/
     
      output.isMissing || isOlder(output) || notHashed(output) || hasDifferentHash(output)
    }
    
    val result = dep.outputs.isEmpty || dep.outputs.exists(needsToBeRun)
    
    //println(s"shouldRun(): $result")
    
    result
  }
  
  private def runWithoutDeps(job: LJob): Future[Result] = {
    val f = job.execute
    
    def toOutputRow(output: Output): OutputRow = {
      //TODO: Smell
      val path = output.asInstanceOf[Output.PathOutput].path
      
      OutputRow(path, output.lastModified, output.hash)
    }
    
    import Traversables.Implicits._
    
    f.foreach { _ =>
      outputs.mutate { oldOutputs =>
        val newOutputs = job.outputs.mapTo(toOutputRow)
        
        oldOutputs ++ newOutputs 
      }
    }
    
    f
  }
  
  private def run(job: LJob): Future[Map[LJob, Result]] = {
    val futureDepResults = runAndMerge(job.inputs)
    
    for {
      depResults <- futureDepResults
      jobResult <- runWithoutDeps(job)
    } yield {
      depResults + (job -> jobResult)
    }
  }
  
  private def runAndMerge(jobs: Iterable[LJob]): Future[Map[LJob, Result]] = {
    val toBeRun = jobs.iterator.filter(shouldRun)
    
    val rawDepResults = toBeRun.map(run)
    
    Future.sequence(rawDepResults).map(Maps.mergeMaps)
  }
}