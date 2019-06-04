package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.drm.Queue
import loamstream.drm.ContainerParams
import java.time.Instant
import loamstream.model.execute.Resources.LocalResources
import loamstream.model.execute.Resources.GoogleResources
import loamstream.model.execute.Resources.DrmResources
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.execute.Resources.LsfResources

/**
 * @author clint
 * May 28, 2019
 */
final class FileSystemExecutionRecorderTest extends FunSuite {
  import FileSystemExecutionRecorder.settingsToString
  import FileSystemExecutionRecorder.resourcesToString
  private val startTime = Instant.now
  
  test("settingsToString - local settings") {
    assert(settingsToString(LocalSettings) === s"settings-type\tlocal")
  }
  
  test("settingsToString - Google settings") {
    assert(settingsToString(GoogleSettings("foo")) === s"settings-type\tgoogle\ncluster\tfoo")
  }
  
  test("settingsToString - DRM settings") {
    def doTest(makeSettings: DrmSettings.SettingsMaker, expectedSettingsType: String): Unit = {
      def doTestWith(queueOpt: Option[Queue], containerParamsOpt: Option[ContainerParams]): Unit = {
        val cores = Cpus(42)
        val memoryPerCore = Memory.inGb(1.23)
        val maxRunTime = CpuTime.inSeconds(345)

        val settings = makeSettings(cores, memoryPerCore, maxRunTime, queueOpt, containerParamsOpt)
      
        val actual = settingsToString(settings)
      
        val expected = Seq(
            s"settings-type\t${expectedSettingsType}",
            s"singularity-image-name\t${settings.containerParams.map(_.imageName).getOrElse("")}",
            s"cores\t${settings.cores.value}",
            s"memory\t${settings.memoryPerCore.value}",
            s"max-run-time\t${settings.maxRunTime.duration}",
            s"queue\t${settings.queue.map(_.toString).getOrElse("")}").mkString("\n")
          
        assert(actual === expected)
      }
      
      val queue = Queue("foo")
      val containerParams = ContainerParams("some-image")
      
      doTestWith(None, None)
      doTestWith(Some(queue), None)
      doTestWith(None, Some(containerParams))
      doTestWith(Some(queue), Some(containerParams))
    }
    
    doTest(UgerDrmSettings.apply, "uger")
    doTest(LsfDrmSettings.apply, "lsf")
  }
  
  test("resourcesToString - local resources") {
    val endTime = Instant.now
    
    val resources = LocalResources(startTime, endTime)
    
    assert(resourcesToString(resources) === s"start-time\t${startTime}\nend-time\t${endTime}")
  }
  
  test("resourcesToString - google resources") {
    val endTime = Instant.now
    
    val resources = GoogleResources("foo", startTime, endTime)
    
    assert(resourcesToString(resources) === s"start-time\t${startTime}\nend-time\t${endTime}\ncluster\tfoo")
  }
  
  test("resourcesToString - DRM resources") {
    val endTime = Instant.now
    
    def doTest[R <: DrmResources](makeResources: DrmResources.ResourcesMaker[R]): Unit = {
      def doTestWith(nodeOpt: Option[String], queueOpt: Option[Queue]): Unit = {
        val memoryPerCore = Memory.inGb(1.23)
        val cpuTime = CpuTime.inSeconds(345)
        
        val resources = makeResources(memoryPerCore, cpuTime, nodeOpt, queueOpt, startTime, endTime, Some("fofofofo"))
        
        val actual = resourcesToString(resources)
        
        val expected = Seq(
            s"start-time\t${resources.startTime}",
            s"end-time\t${resources.endTime}",
            s"memory\t${resources.memory.value}",
            s"cpu-time\t${resources.cpuTime.duration}",
            s"execution-node\t${resources.node.getOrElse("")}",
            s"queue\t${resources.queue.map(_.toString).getOrElse("")}").mkString("\n")
        
        assert(actual === expected)
      }
      
      val queue = Queue("foo")
      val node = "foo.example.com"
      
      doTestWith(None, None)
      doTestWith(None, Some(queue))
      doTestWith(Some(node), None)
      doTestWith(Some(node), Some(queue))
    }

    doTest(UgerResources.apply)
    doTest(LsfResources.apply)
  }
}
