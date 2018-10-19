package loamstream.drm.uger

import loamstream.drm.NativeSpecBuilder
import loamstream.model.execute.DrmSettings
import loamstream.model.execute.UgerDrmSettings
import loamstream.conf.UgerConfig

/**
 * @author clint
 * May 11, 2018
 */
final case class UgerNativeSpecBuilder(ugerConfig: UgerConfig) extends NativeSpecBuilder {
  override def toNativeSpec(ugerSettings: DrmSettings): String = {

    val staticPart = ugerConfig.staticJobSubmissionParams

    val dynamicPart = {
      import ugerSettings._

      val numCores = cores.value
      val runTimeInHours: Int = maxRunTime.hours.toInt
      val mem: Int = memoryPerCore.gb.toInt

      val queuePart = queue.map(q => s"-q $q").getOrElse("")
      
      val osPart = ugerSettings.containerParams match {
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
