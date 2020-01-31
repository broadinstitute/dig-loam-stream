package loamstream.apps

import loamstream.util.TimeUtils
import scala.util.Try
import loamstream.model.jobs.LJob
import loamstream.model.execute.ExecutionCell
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.execute.LocalSettings

object Foo extends App {
  def time(title: String, warmupRuns: Int = 100)(f: => Any): Unit = {
    def run(): (Try[_], Long) = {
      TimeUtils.elapsed(f)
      TimeUtils.elapsed(f)
      TimeUtils.elapsed(f)
      TimeUtils.elapsed(f)
      TimeUtils.elapsed(f)
      TimeUtils.elapsed(f)
      TimeUtils.elapsed(f)
    }
    
    var t: AnyRef = new AnyRef
    
    (1 to warmupRuns).foreach(i => { println(s"'$title' warmup #$i") ; t = run() })
    
    val (_, elapsed) = run()
    
    println(s"$title: $elapsed ms")
  }
  
  val N = 100000
  
  def clj(i: Int): CommandLineJob = CommandLineJob(
    commandLineString = s"foo --bar $i",
    initialSettings = LocalSettings)
  
  val m = Map.empty[LJob, ExecutionCell] ++ (1 to N).map { i =>
    clj(i) -> ExecutionCell.initial
  }
  
  val a: Array[(LJob, ExecutionCell)] = (1 to N).map { i => (clj(i) -> ExecutionCell.initial) }.toArray
    
  var n = 0
  
  time("Map") {
    n += m.count { case (j, _) => j eq j }
  }
  
  time("Array") {
    n += a.count { case (j, _) => j eq j }
  }
}
