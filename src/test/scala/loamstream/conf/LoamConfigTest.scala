package loamstream.conf

import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory
import loamstream.TestHelpers
import org.broadinstitute.dig.aws.config.AWSConfig
import org.broadinstitute.dig.aws.config.S3Config
import org.broadinstitute.dig.aws.config.emr.EmrConfig
import org.broadinstitute.dig.aws.config.emr.SubnetId
import org.broadinstitute.dig.aws.config.emr.ReleaseLabel
import org.broadinstitute.dig.aws.config.emr.SecurityGroupId
import org.broadinstitute.dig.aws.config.emr.RoleId

/**
 * @author clint
 * Sep 17, 2019
 */
final class LoamConfigTest extends FunSuite {
  test("fromConfig - defaults") {
    val config = ConfigFactory.parseString("loamstream { }")
    
    val parsed = LoamConfig.fromConfig(config).get
    
    val expected = LoamConfig(
      ugerConfig = Some(UgerConfig()),
      lsfConfig = Some(LsfConfig()),
      googleConfig = None,
      hailConfig = None,
      pythonConfig = None,
      rConfig = None,
      executionConfig = ExecutionConfig.default,
      compilationConfig = CompilationConfig.default,
      awsConfig = None,
      drmSystem = None)
      
    assert(parsed === expected)
  }
  
  test("fromConfig - defaults except for AWS config") {
    val config = ConfigFactory.parseString("""
    loamstream {
      aws {
        s3 { bucket = "some-bucket" }
        emr {
          sshKeyName = "some-ssh-key-name"
          subnetId = "subnet-some-subnet-id"
          releaseLabel = "emr-some-release-label"
          securityGroupIds = ["sg-foo", "sg-bar"]
          serviceRoleId = "some-role-id"
          jobFlowRoleId = "some-other-role-id"
          autoScalingRoleId = "yet-another-role-id"
        }
      } 
    }""")
    
    val parsed = LoamConfig.fromConfig(config).get
    
    import TestHelpers.path
    
    val expected = LoamConfig(
      ugerConfig = Some(UgerConfig()),
      lsfConfig = Some(LsfConfig()),
      googleConfig = None,
      hailConfig = None,
      pythonConfig = None,
      rConfig = None,
      executionConfig = ExecutionConfig.default,
      compilationConfig = CompilationConfig.default,
      awsConfig = Some(AWSConfig(
        s3 = S3Config("some-bucket"),
        emr = EmrConfig(
          sshKeyName = "some-ssh-key-name",
          subnetId = SubnetId("subnet-some-subnet-id"),
          releaseLabel = ReleaseLabel("emr-some-release-label"),
          securityGroupIds = Seq(SecurityGroupId("sg-foo"), SecurityGroupId("sg-bar")),
          serviceRoleId = RoleId("some-role-id"),
          jobFlowRoleId = RoleId("some-other-role-id"),
          autoScalingRoleId = RoleId("yet-another-role-id")))),
      drmSystem = None)
      
    assert(parsed === expected)
  }
}
