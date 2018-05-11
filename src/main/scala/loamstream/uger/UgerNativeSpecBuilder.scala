package loamstream.uger

import loamstream.drm.NativeSpecBuilder
import loamstream.model.execute.DrmSettings

/**
 * @author clint
 * May 11, 2018
 */
object UgerNativeSpecBuilder extends NativeSpecBuilder {
  override def toNativeSpec(drmSettings: DrmSettings): String = {
    //Will this ever change?
    val staticPart = "-cwd -shell y -b n"

    val dynamicPart = {
      import drmSettings._

      val numCores = cores.value
      val runTimeInHours: Int = maxRunTime.hours.toInt
      val mem: Int = memoryPerCore.gb.toInt

      val queuePart = queue.map(q => s"-q $q").getOrElse("")
      
      s"-binding linear:${numCores} -pe smp ${numCores} $queuePart -l h_rt=${runTimeInHours}:0:0,h_vmem=${mem}g"
    }

    s"$staticPart $dynamicPart"
  }
}
