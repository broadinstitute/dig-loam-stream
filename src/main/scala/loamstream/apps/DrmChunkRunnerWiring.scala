package loamstream.apps

import monix.execution.Scheduler
import loamstream.drm.DrmChunkRunner
import loamstream.util.Terminable
import loamstream.conf.LoamConfig
import loamstream.drm.uger.UgerPathBuilder
import loamstream.drm.lsf.LsfPathBuilder
import loamstream.drm.uger.QacctAccountingClient
import loamstream.drm.lsf.BjobsPoller
import loamstream.drm.lsf.BacctAccountingClient
import loamstream.drm.uger.QsubJobSubmitter
import loamstream.drm.lsf.BsubJobSubmitter
import loamstream.drm.uger.UgerScriptBuilderParams
import loamstream.model.execute.EnvironmentType
import loamstream.drm.JobMonitor
import loamstream.drm.uger.QdelJobKiller
import loamstream.drm.lsf.BkillJobKiller
import loamstream.drm.uger.QstatPoller
import loamstream.drm.SessionTracker
import loamstream.util.ExitCodes
import loamstream.util.Loggable
import loamstream.drm.slurm.SqueuePoller
import loamstream.drm.slurm.SbatchJobSubmitter
import loamstream.drm.slurm.SacctAccountingClient
import loamstream.drm.slurm.ScancelJobKiller
import loamstream.drm.slurm.SlurmPathBuilder

/**
 * @author clint
 *
 * Jun 7, 2021
 */
object DrmChunkRunnerWiring extends Loggable {
  object Defaults {
    val pollingFrequencyInHz = 0.1
  }
  
  private[apps] def makeUgerChunkRunner(
    loamConfig: LoamConfig,
    scheduler: Scheduler): Option[(DrmChunkRunner, Seq[Terminable])] = {

    for {
      ugerConfig <- loamConfig.ugerConfig
    } yield {
      debug("Creating Uger ChunkRunner...")

      //TODO: Make configurable?
      val pollingFrequencyInHz = Defaults.pollingFrequencyInHz

      val poller = QstatPoller.fromExecutable(pollingFrequencyInHz, loamConfig.executionConfig, scheduler = scheduler)

      val jobMonitor = new JobMonitor(scheduler, poller, pollingFrequencyInHz)

      val jobSubmitter = QsubJobSubmitter.fromExecutable(ugerConfig)(scheduler = scheduler)

      val accountingClient = QacctAccountingClient.useActualBinary(ugerConfig, scheduler)

      val sessionTracker: SessionTracker = new SessionTracker.Default

      val jobKiller = QdelJobKiller.fromExecutable(sessionTracker, ugerConfig, isSuccess = Set(0, 1).contains)

      val ugerRunner = DrmChunkRunner(
        environmentType = EnvironmentType.Uger,
        pathBuilder = new UgerPathBuilder(UgerScriptBuilderParams(ugerConfig)),
        executionConfig = loamConfig.executionConfig,
        drmConfig = ugerConfig,
        jobSubmitter = jobSubmitter,
        jobMonitor = jobMonitor,
        accountingClient = accountingClient,
        jobKiller = jobKiller,
        sessionTracker = sessionTracker)

      (ugerRunner, Seq(ugerRunner))
    }
  }

  private[apps] def makeLsfChunkRunner(
    loamConfig: LoamConfig,
    scheduler: Scheduler): Option[(DrmChunkRunner, Seq[Terminable])] = {

    for {
      lsfConfig <- loamConfig.lsfConfig
    } yield {
      debug("Creating LSF ChunkRunner...")

      //TODO: Make configurable?
      val pollingFrequencyInHz = Defaults.pollingFrequencyInHz

      val poller = BjobsPoller.fromExecutable()

      val jobMonitor = new JobMonitor(scheduler, poller, pollingFrequencyInHz)

      val jobSubmitter = BsubJobSubmitter.fromExecutable(lsfConfig)

      val accountingClient = BacctAccountingClient.useActualBinary(lsfConfig, scheduler)

      val sessionTracker: SessionTracker = new SessionTracker.Default

      val jobKiller = BkillJobKiller.fromExecutable(sessionTracker, lsfConfig, isSuccess = ExitCodes.isSuccess)

      val lsfRunner = DrmChunkRunner(
        environmentType = EnvironmentType.Lsf,
        pathBuilder = LsfPathBuilder,
        executionConfig = loamConfig.executionConfig,
        drmConfig = lsfConfig,
        jobSubmitter = jobSubmitter,
        jobMonitor = jobMonitor,
        accountingClient = accountingClient,
        jobKiller = jobKiller,
        sessionTracker = sessionTracker)

      (lsfRunner, Seq(lsfRunner))
    }
  }

  private[apps] def makeSlurmChunkRunner(
    loamConfig: LoamConfig,
    scheduler: Scheduler): Option[(DrmChunkRunner, Seq[Terminable])] = {

    for {
      slurmConfig <- loamConfig.slurmConfig
    } yield {
      debug("Creating SLURM ChunkRunner...")

      //TODO: Make configurable?
      val pollingFrequencyInHz = Defaults.pollingFrequencyInHz

      val poller = SqueuePoller.fromExecutable(
          "squeue",
          pollingFrequencyInHz,
          loamConfig.executionConfig,
          scheduler)

      val jobMonitor = new JobMonitor(scheduler, poller, pollingFrequencyInHz)

      val jobSubmitter = SbatchJobSubmitter.fromExecutable(slurmConfig)(scheduler)

      val accountingClient = SacctAccountingClient.useActualBinary(slurmConfig, scheduler)

      val sessionTracker: SessionTracker = new SessionTracker.Default

      val jobKiller = ScancelJobKiller.fromExecutable(sessionTracker, slurmConfig, isSuccess = ExitCodes.isSuccess)

      val slurmRunner = DrmChunkRunner(
        environmentType = EnvironmentType.Slurm,
        pathBuilder = SlurmPathBuilder,
        executionConfig = loamConfig.executionConfig,
        drmConfig = slurmConfig,
        jobSubmitter = jobSubmitter,
        jobMonitor = jobMonitor,
        accountingClient = accountingClient,
        jobKiller = jobKiller,
        sessionTracker = sessionTracker)

      (slurmRunner, Seq(slurmRunner))
    }
  }
}