package loamstream.model.execute

import loamstream.model.jobs.LJob
import loamstream.util.Shot

/**
 * RugLoom - A prototype for a pipeline building toolkit
 * Created by oruebenacker on 2/24/16.
 */
trait LExecuter {

  def execute(executable:LExecutable): Map[LJob,Shot[LJob.Result]]

}
