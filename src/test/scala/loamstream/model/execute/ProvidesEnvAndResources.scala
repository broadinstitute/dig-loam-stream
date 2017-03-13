package loamstream.model.execute

import loamstream.model.execute.ExecutionEnvironment.Local

/**
 * @author kyuksel
 *         date: 3/11/17
 */
trait ProvidesEnvAndResources {
  val mockEnv: ExecutionEnvironment = Local
  val mockSettings: Settings = new LocalSettings
  val mockResources: Resources = LocalResources(None, None)
}
