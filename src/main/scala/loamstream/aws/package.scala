package loamstream

import org.broadinstitute.dig.aws.JobStep

/**
 * @author clint
 * Oct 24, 2019
 */
package object aws {
  type AwsJobDesc = Seq[JobStep]
}