package loamstream.util.logback

import ch.qos.logback.core.rolling.TriggeringPolicyBase
import ch.qos.logback.core.joran.spi.NoAutoStart
import java.io.File

@NoAutoStart
final class OnStartupTriggeringPolicy[A] extends TriggeringPolicyBase[A] {
  @volatile private[this] var triggerRollover: Boolean = true
  
  override def isTriggeringEvent(activeFile: File, event: A): Boolean = {
    if (!triggerRollover) { false }
    else {
      triggerRollover = false
    
      true
    }
  }
}
