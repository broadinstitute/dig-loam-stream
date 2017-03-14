package loamstream.model.execute

import loamstream.model.execute.ExecutionEnvironment.Local

/**
 * @author kyuksel
 *         date: 3/11/17
 */
trait ProvidesEnvAndResources {
  protected val mockEnv: ExecutionEnvironment = Local
  protected val mockSettings: Settings = new LocalSettings
  protected val mockResources: Resources = LocalResources(None, None)
}
