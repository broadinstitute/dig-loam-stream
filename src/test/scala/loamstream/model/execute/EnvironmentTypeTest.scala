package loamstream.model.execute

import org.scalatest.FunSuite
import Resources._
import java.time.LocalDateTime
import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime

/**
 * @author clint
 * Oct 12, 2017
 */
final class EnvironmentTypeTest extends FunSuite {
  import EnvironmentType._

  test("matches - everything 'matches' no resources") {
    assert(Local.matches(None) === true)
    assert(Google.matches(None) === true)
    assert(Uger.matches(None) === true)
    assert(Lsf.matches(None) === true)
    assert(Slurm.matches(None) === true)
    assert(Aws.matches(None) === true)
  }

  private val localResources = LocalResources(LocalDateTime.now, LocalDateTime.now)
  private val googleResources = GoogleResources("some-cluster", LocalDateTime.now, LocalDateTime.now)
  private val ugerResources = UgerResources(
    memory = Memory.inKb(123),
    cpuTime = CpuTime.inSeconds(123),
    node = Some("some-node"),
    queue = None,
    startTime = LocalDateTime.now,
    endTime = LocalDateTime.now,
    raw = Some("lalala"))

  private val lsfResources = LsfResources(
    memory = Memory.inKb(123),
    cpuTime = CpuTime.inSeconds(123),
    node = Some("some-node"),
    queue = None,
    startTime = LocalDateTime.now,
    endTime = LocalDateTime.now,
    raw = Some("lalala"))
  
  private val slurmResources = SlurmResources(
    memory = Memory.inKb(123),
    cpuTime = CpuTime.inSeconds(123),
    node = Some("some-node"),
    queue = None,
    startTime = LocalDateTime.now,
    endTime = LocalDateTime.now,
    raw = Some("lalala"))

  private val awsResources = AwsResources("some-cluster", LocalDateTime.now, LocalDateTime.now)

  test("matches - some resources") {
    assert(Local.matches(Some(localResources)) === true)
    assert(Local.matches(Some(googleResources)) === false)
    assert(Local.matches(Some(ugerResources)) === false)
    assert(Local.matches(Some(lsfResources)) === false)
    assert(Local.matches(Some(slurmResources)) === false)
    assert(Local.matches(Some(awsResources)) === false)

    assert(Google.matches(Some(localResources)) === false)
    assert(Google.matches(Some(googleResources)) === true)
    assert(Google.matches(Some(ugerResources)) === false)
    assert(Google.matches(Some(lsfResources)) === false)
    assert(Google.matches(Some(slurmResources)) === false)
    assert(Google.matches(Some(awsResources)) === false)

    assert(Uger.matches(Some(localResources)) === false)
    assert(Uger.matches(Some(googleResources)) === false)
    assert(Uger.matches(Some(ugerResources)) === true)
    assert(Uger.matches(Some(lsfResources)) === false)
    assert(Uger.matches(Some(slurmResources)) === false)
    assert(Uger.matches(Some(awsResources)) === false)

    assert(Lsf.matches(Some(localResources)) === false)
    assert(Lsf.matches(Some(googleResources)) === false)
    assert(Lsf.matches(Some(ugerResources)) === false)
    assert(Lsf.matches(Some(lsfResources)) === true)
    assert(Lsf.matches(Some(slurmResources)) === false)
    assert(Lsf.matches(Some(awsResources)) === false)

    assert(Slurm.matches(Some(localResources)) === false)
    assert(Slurm.matches(Some(googleResources)) === false)
    assert(Slurm.matches(Some(ugerResources)) === false)
    assert(Slurm.matches(Some(lsfResources)) === false)
    assert(Slurm.matches(Some(slurmResources)) === true)
    assert(Slurm.matches(Some(awsResources)) === false)

    assert(Aws.matches(Some(localResources)) === false)
    assert(Aws.matches(Some(googleResources)) === false)
    assert(Aws.matches(Some(ugerResources)) === false)
    assert(Aws.matches(Some(lsfResources)) === false)
    assert(Aws.matches(Some(slurmResources)) === false)
    assert(Aws.matches(Some(awsResources)) === true)
  }
  
  test("name") {
    assert(Local.name === "local")
    assert(Google.name === "google")
    assert(Uger.name === "uger")
    assert(Lsf.name === "lsf")
    assert(Slurm.name === "slurm")
    assert(Aws.name === "aws")
  }
  
  test("is* predicates") {
    assert(Local.isLocal === true)
    assert(Local.isGoogle === false)
    assert(Local.isUger === false)
    assert(Local.isLsf === false)
    assert(Local.isSlurm === false)
    assert(Local.isAws === false)
    assert(Local.isDrm === false)
    
    assert(Google.isLocal === false)
    assert(Google.isGoogle === true)
    assert(Google.isUger === false)
    assert(Google.isLsf === false)
    assert(Google.isSlurm === false)
    assert(Google.isAws === false)
    assert(Google.isDrm === false)
    
    assert(Uger.isLocal === false)
    assert(Uger.isGoogle === false)
    assert(Uger.isUger === true)
    assert(Uger.isLsf === false)
    assert(Uger.isSlurm === false)
    assert(Uger.isAws === false)
    assert(Uger.isDrm === true)
    
    assert(Lsf.isLocal === false)
    assert(Lsf.isGoogle === false)
    assert(Lsf.isUger === false)
    assert(Lsf.isLsf === true)
    assert(Lsf.isSlurm === false)
    assert(Lsf.isAws === false)
    assert(Lsf.isDrm === true)
    
    assert(Slurm.isLocal === false)
    assert(Slurm.isGoogle === false)
    assert(Slurm.isUger === false)
    assert(Slurm.isLsf === false)
    assert(Slurm.isSlurm === true)
    assert(Slurm.isAws === false)
    assert(Slurm.isDrm === true)
    
    assert(Aws.isLocal === false)
    assert(Aws.isGoogle === false)
    assert(Aws.isUger === false)
    assert(Aws.isLsf === false)
    assert(Aws.isSlurm === false)
    assert(Aws.isAws === true)
    assert(Aws.isDrm === false)
  }
  
  test("fromString") {
    assert(fromString("") === None)
    assert(fromString("   ") === None)
    assert(fromString("asdff") === None)
    
    assert(fromString("local") === Some(Local))
    assert(fromString("Local") === Some(Local))
    assert(fromString("LOCAL") === Some(Local))
    assert(fromString("LoCaL") === Some(Local))
    
    assert(fromString("google") === Some(Google))
    assert(fromString("Google") === Some(Google))
    assert(fromString("GOOGLE") === Some(Google))
    assert(fromString("GoOgLe") === Some(Google))
    
    assert(fromString("uger") === Some(Uger))
    assert(fromString("Uger") === Some(Uger))
    assert(fromString("UGER") === Some(Uger))
    assert(fromString("UgEr") === Some(Uger))
    
    assert(fromString("lsf") === Some(Lsf))
    assert(fromString("Lsf") === Some(Lsf))
    assert(fromString("LSF") === Some(Lsf))
    assert(fromString("LsF") === Some(Lsf))
    
    assert(fromString("slurm") === Some(Slurm))
    assert(fromString("Slurm") === Some(Slurm))
    assert(fromString("SLURM") === Some(Slurm))
    assert(fromString("SlUrM") === Some(Slurm))
    
    assert(fromString("aws") === Some(Aws))
    assert(fromString("Aws") === Some(Aws))
    assert(fromString("AWS") === Some(Aws))
    assert(fromString("AwS") === Some(Aws))
  }
}
