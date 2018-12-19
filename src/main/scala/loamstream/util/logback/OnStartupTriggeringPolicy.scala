package loamstream.util.logback

import ch.qos.logback.core.rolling.TriggeringPolicyBase
import ch.qos.logback.core.joran.spi.NoAutoStart
import java.io.File

/**
 * A class to make Slf4j/Logback create a new log file on app startup, while keeping
 * some number of previous logs.  See src/main/resources/logback.xml
 */
@NoAutoStart
final class OnStartupTriggeringPolicy[A] extends TriggeringPolicyBase[A] {
  @volatile private[this] var triggerRollover: Boolean = true
  
  //This call must happen or else no logging will occur via appenders that use this policy.
  start()
  
  override def isTriggeringEvent(activeFile: File, event: A): Boolean = {
    if (!triggerRollover) { false }
    else {
      triggerRollover = false
    
      true
    }
  }
}
