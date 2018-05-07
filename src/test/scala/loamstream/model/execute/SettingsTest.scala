package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.uger.UgerDefaults
import loamstream.drm.Queue
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime

/**
 * @author clint
 * Oct 12, 2017
 */
final class SettingsTest extends FunSuite {
  test("UgerSettings constructor defaults") {
    {
      val lotsOfCpus = Cpus(42)
      val seventeenGigs = Memory.inGb(17)
      
      val withDefaults = UgerSettings(lotsOfCpus, seventeenGigs)
      
      assert(withDefaults.cores === lotsOfCpus)
      assert(withDefaults.memoryPerCore === seventeenGigs)
      assert(withDefaults.maxRunTime === UgerDefaults.maxRunTime)
      assert(withDefaults.queue === UgerDefaults.queue)
    }
    
    {
      val lotsOfCpus = Cpus(33)
      val nineteenGigs = Memory.inGb(19)
      val elevenHours = CpuTime.inHours(11)
    
      val withMaxRunTime = UgerSettings(lotsOfCpus, nineteenGigs, elevenHours)
      
      assert(withMaxRunTime.cores === lotsOfCpus)
      assert(withMaxRunTime.memoryPerCore === nineteenGigs)
      assert(withMaxRunTime.maxRunTime === elevenHours)
      assert(withMaxRunTime.queue === UgerDefaults.queue)
    }
  }
}
