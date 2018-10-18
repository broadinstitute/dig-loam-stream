package loamstream.drm.uger

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
      
      val osPart = drmSettings.containerParams match {
        case Some(_) => "-l os=RedHat7"
        case None => ""
      }
      
      val memPart = s"h_vmem=${mem}g"
      
      val runTimePart = s"h_rt=${runTimeInHours}:0:0"
      
      s"-binding linear:${numCores} -pe smp ${numCores} $queuePart -l ${runTimePart},${memPart} ${osPart}"
    }

    s"$staticPart $dynamicPart"
  }
}
