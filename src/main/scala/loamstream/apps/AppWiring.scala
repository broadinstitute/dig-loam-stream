package loamstream.apps

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import java.nio.file.Path

import loamstream.cli.Conf
import loamstream.cli.Intent
import loamstream.cli.Intent.RealRun
import loamstream.cli.JobFilterIntent
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.conf.ExecutionConfig
import loamstream.conf.LoamConfig
import loamstream.conf.Locations
import loamstream.conf.LsSettings
import loamstream.conf.PythonConfig
import loamstream.conf.RConfig
import loamstream.conf.UgerConfig

import loamstream.db.LoamDao
import loamstream.db.slick.DbDescriptor
import loamstream.db.slick.DbType
import loamstream.db.slick.SlickLoamDao

import loamstream.drm.AccountingClient
import loamstream.drm.DrmChunkRunner
import loamstream.drm.DrmSystem
import loamstream.drm.JobMonitor
import loamstream.drm.JobSubmitter
import loamstream.drm.Poller
import loamstream.drm.SessionSource

import loamstream.drm.lsf.BacctAccountingClient
import loamstream.drm.lsf.BjobsPoller
import loamstream.drm.lsf.BkillJobKiller
import loamstream.drm.lsf.BsubJobSubmitter
import loamstream.drm.lsf.LsfPathBuilder

import loamstream.drm.uger.UgerScriptBuilderParams
import loamstream.drm.uger.QacctAccountingClient
import loamstream.drm.uger.QdelJobKiller
import loamstream.drm.uger.QstatQacctPoller
import loamstream.drm.uger.Qsub
import loamstream.drm.uger.QsubJobSubmitter
import loamstream.drm.uger.QconfSessionSource
import loamstream.drm.uger.QconfSessionSource
import loamstream.drm.uger.UgerPathBuilder

import loamstream.googlecloud.CloudSdkDataProcWrapper
import loamstream.googlecloud.CloudStorageClient
import loamstream.googlecloud.GcsCloudStorageClient
import loamstream.googlecloud.GcsCloudStorageDriver
import loamstream.googlecloud.GoogleCloudConfig
import loamstream.googlecloud.GoogleCloudChunkRunner
import loamstream.googlecloud.HailConfig
import loamstream.googlecloud.HailCtlDataProcClient

import loamstream.model.execute.AsyncLocalChunkRunner
import loamstream.model.execute.ChunkRunner
import loamstream.model.execute.CompositeChunkRunner
import loamstream.model.execute.DbBackedJobFilter
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.Executable
import loamstream.model.execute.Executer
import loamstream.model.execute.HashingStrategy
import loamstream.model.execute.JobFilter
import loamstream.model.execute.RxExecuter
import loamstream.model.execute.ByNameJobFilter
import loamstream.model.execute.DbBackedExecutionRecorder
import loamstream.model.execute.ExecutionRecorder
import loamstream.model.execute.FileSystemExecutionRecorder
import loamstream.model.execute.JobCanceler
import loamstream.model.execute.RequiresPresentInputsJobCanceler
import loamstream.model.execute.RunsIfNoOutputsJobFilter

import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobOracle
import loamstream.model.jobs.LJob

import loamstream.util.ConfigUtils
import loamstream.util.ExecutionContexts
import loamstream.util.ExitCodes
import loamstream.util.FileMonitor
import loamstream.util.Loggable
import loamstream.util.RxSchedulers
import loamstream.util.Terminable
import loamstream.util.ThisMachine
import loamstream.util.Throwables
import loamstream.util.Tries

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Try
import scala.util.Success

import rx.lang.scala.Scheduler
import rx.lang.scala.schedulers.ExecutionContextScheduler
import loamstream.util.DirOracle
import loamstream.model.execute.ProtectsFilesJobCanceler
import loamstream.model.execute.SuccessfulOutputsExecutionRecorder



/**
 * @author clint
 *         kyuksel
 * Nov 10, 2016
 */
trait AppWiring {
  def config: LoamConfig
  
  def settings: LsSettings
  
  def dao: LoamDao

  def executer: Executer

  def cloudStorageClient: Option[CloudStorageClient]
  
  def jobFilter: JobFilter
  
  def executionRecorder: ExecutionRecorder
  
  def shutdown(): Seq[Throwable]
  
  lazy val loamEngine: LoamEngine = LoamEngine(config, settings, LoamCompiler.default, executer, cloudStorageClient)
  
  lazy val loamRunner: LoamRunner = LoamRunner(loamEngine)
}

object AppWiring extends Loggable {

  def loamConfigFrom(
      confFile: Option[Path], 
      drmSystemOpt: Option[DrmSystem], 
      shouldValidateGraph: Boolean,
      cliConfig: Option[Conf]): LoamConfig = {
    
    val typesafeConfig: Config = loadConfig(confFile)
      
    //TODO: Revisit .get
    val withoutDrmSystem = LoamConfig.fromConfig(typesafeConfig).get
    
    val withDrmSystem = withoutDrmSystem.copy(drmSystem = drmSystemOpt)
    
    val newCompilationConfig = withDrmSystem.compilationConfig.copy(shouldValidateGraph = shouldValidateGraph)
    
    withDrmSystem.copy(compilationConfig = newCompilationConfig).copy(cliConfig = cliConfig)
  }
  
  def jobFilterForDryRun(intent: Intent.DryRun, config: LoamConfig, makeDao: LoamConfig => LoamDao): JobFilter = {
    AppWiring.makeJobFilter(intent.jobFilterIntent, intent.hashingStrategy, makeDao(config))
  }
  
  def forRealRun(intent: Intent.RealRun, makeDao: LoamConfig => LoamDao): AppWiring = {
    new DefaultAppWiring(intent, makeDao = makeDao)
  }

  private[apps] def makeJobFilter(
      jobFilterIntent: JobFilterIntent,
      hashingStrategy: HashingStrategy,
      getDao: => LoamDao): JobFilter = {
    
    import JobFilterIntent._
    
    jobFilterIntent match {
      case convertible: JobFilterIntent.ConvertibleToJobFilter => convertible.toJobFilter
      case _ => defaultJobFilter(getDao, hashingStrategy)
    }
  }
  
  private[AppWiring] def makeExecutionRecorder(
      executionConfig: ExecutionConfig)(getDao: => LoamDao): ExecutionRecorder = {
    
    val successfulOutputFile = executionConfig.locations.logDir.resolve("successful-job-outputs.txt")
    
    FileSystemExecutionRecorder && 
    (new DbBackedExecutionRecorder(getDao)) &&
    SuccessfulOutputsExecutionRecorder(successfulOutputFile)
  }
  
  private final class DefaultAppWiring(
      intent: Intent.RealRun,
      makeDao: LoamConfig => LoamDao) extends AppWiring {
    
    override lazy val dao: LoamDao = makeDao(config)
    
    override lazy val config: LoamConfig = {
      loamConfigFrom(
          confFile = intent.confFile, 
          drmSystemOpt = intent.drmSystemOpt, 
          shouldValidateGraph = intent.shouldValidate,
          cliConfig = intent.cliConfig)
    }
    
    def cliConfigValues: Option[Conf.Values] = intent.cliConfig.map(_.toValues)
    
    val settings: LsSettings = {
      require(intent.cliConfig.isDefined, "Expected LS to be run from the command line")
      
      LsSettings(cliConfigValues)
    }
    
    override def executer: Executer = terminableExecuter

    override def shutdown(): Seq[Throwable] = terminableExecuter.shutdown()

    override lazy val cloudStorageClient: Option[CloudStorageClient] = makeCloudStorageClient(intent.confFile, config)

    override lazy val jobFilter: JobFilter = makeJobFilter(intent.jobFilterIntent, intent.hashingStrategy, dao)
    
    override lazy val executionRecorder: ExecutionRecorder = makeExecutionRecorder(config.executionConfig)(dao)
    
    private lazy val terminableExecuter: TerminableExecuter = {
      trace("Creating executer...")

      val threadPoolSize = config.executionConfig.numWorkerThreads

      val (executionContextWithThreadPool, threadPoolHandle) = {
        ExecutionContexts.threadPool(threadPoolSize, s"LS-mainWorkerPool")
      }
      
      val (compositeRunner: ChunkRunner, runnerHandles: Seq[Terminable]) = {
        makeChunkRunner(executionContextWithThreadPool)
      }

      import loamstream.model.execute.ExecuterHelpers._
      
      val scheduler: Scheduler = ExecutionContextScheduler(executionContextWithThreadPool)

      import scala.concurrent.duration._
      
      val windowLength = (1.0 / config.executionConfig.executionPollingFrequencyInHz).seconds
      
      import config.executionConfig.{ maxRunsPerJob, maxWaitTimeForOutputs, outputPollingFrequencyInHz }
      
      val rxExecuter = {
        RxExecuter(
            config.executionConfig,
            compositeRunner, 
            new FileMonitor(outputPollingFrequencyInHz, maxWaitTimeForOutputs),
            windowLength, 
            defaultJobCanceller(cliConfigValues.flatMap(_.protectedOutputsFile)),
            jobFilter, 
            executionRecorder,
            maxRunsPerJob,
            scheduler = scheduler)(executionContextWithThreadPool)
      }

      val handles: Seq[Terminable] = threadPoolHandle +: runnerHandles 

      new TerminableExecuter(rxExecuter, handles: _*)
    }
    
    private def makeChunkRunner(executionContext: ExecutionContext): (ChunkRunner, Seq[Terminable]) = {
      
      //TODO: Make the number of threads this uses configurable
      val numberOfCPUs = ThisMachine.numCpus

      val (localEC, localEcHandle) = ExecutionContexts.threadPool(numberOfCPUs * 2, "LS-localJobsPool")

      val localRunner = AsyncLocalChunkRunner(config.executionConfig)(localEC)

      val (drmRunner, drmRunnerHandles) = drmChunkRunner(intent.confFile, config, executionContext)
      
      val googleRunner = googleChunkRunner(intent.confFile, config.googleConfig, config.hailConfig, localRunner)

      val compositeRunner = CompositeChunkRunner(localRunner +: (drmRunner.toSeq ++ googleRunner))
      
      val toBeStopped = compositeRunner +: localEcHandle +: (drmRunnerHandles ++ compositeRunner.components)
      
      (compositeRunner, toBeStopped.distinct)
    }
  }
  
  private[apps] def determineHashingStrategy(conf: Conf): HashingStrategy = {
    import HashingStrategy.{DontHashOutputs, HashOutputs}
    
    conf.disableHashing.toOption.map { shouldDisableHashing =>
      if(shouldDisableHashing) DontHashOutputs else HashOutputs
    }.getOrElse {
      HashOutputs
    }
  }

  private def googleChunkRunner(
      confFile: Option[Path],
      googleConfigOpt: Option[GoogleCloudConfig],
      hailConfigOpt: Option[HailConfig], 
      delegate: ChunkRunner): Option[GoogleCloudChunkRunner] = {
    
    //TODO: A better way to enable or disable Google support; for now, this is purely expedient
    
    def noConfigs: Option[GoogleCloudChunkRunner] = {
      debug("Google Cloud support NOT enabled due to missing 'loamstream.googlecloud' section in the config file")
        
      None
    }
    
    def configsPresent(configTuple: (GoogleCloudConfig, HailConfig)): Option[GoogleCloudChunkRunner] = {
      val (googleConfig, hailConfig) = configTuple
      
      val runnerAttempt: Try[GoogleCloudChunkRunner] = {
        for {
          client <- HailCtlDataProcClient.fromConfigs(googleConfig, hailConfig)
        } yield {
          trace("Creating Google Cloud ChunkRunner...")
    
          GoogleCloudChunkRunner(client, googleConfig, delegate)
        }
      }

      //NB: Invoke .recover for the logging side effect only :\
      runnerAttempt.recover { case e =>
        val msg = s"""|Google Cloud support NOT enabled because ${e.getMessage}
                      |in the config file (${confFile})""".stripMargin
      
        debug(msg)
      }
    
      runnerAttempt.toOption
    }
    
    val configsOpt = for {
      googleConfig <- googleConfigOpt 
      hailConfig <- hailConfigOpt
    } yield (googleConfig, hailConfig)
    
    configsOpt.fold(noConfigs)(configsPresent)
  }
  
  private def drmChunkRunner(
      confFile: Option[Path], 
      loamConfig: LoamConfig, 
      executionContext: ExecutionContext): (Option[DrmChunkRunner], Seq[Terminable]) = {

    loamConfig.drmSystem match {
      case Some(DrmSystem.Uger) => ugerChunkRunner(confFile, loamConfig, executionContext)
      case Some(DrmSystem.Lsf) => lsfChunkRunner(confFile, loamConfig, executionContext)
      case None => (None, Nil)
    }
  }
  
  private def ugerChunkRunner(
      confFile: Option[Path], 
      loamConfig: LoamConfig, 
      executionContext: ExecutionContext): (Option[DrmChunkRunner], Seq[Terminable]) = {
    
    val result @ (ugerRunnerOption, _) = unpack(makeUgerChunkRunner(loamConfig, executionContext))

    //TODO: A better way to enable or disable Uger support; for now, this is purely expedient
    if(ugerRunnerOption.isEmpty) {
      val msg = s"""Uger support is NOT enabled. It can be enabled by defining loamstream.uger section
                   |in the config file (${confFile}).""".stripMargin
        
      debug(msg)
    }
    
    result
  }
  
  private def lsfChunkRunner(
      confFile: Option[Path], 
      loamConfig: LoamConfig, 
      executionContext: ExecutionContext): (Option[DrmChunkRunner], Seq[Terminable]) = {
    
    val (lsfRunnerOption, terminables) = unpack(makeLsfChunkRunner(loamConfig, executionContext))
    
    //TODO: A better way to enable or disable Uger support; for now, this is purely expedient
    if(lsfRunnerOption.isEmpty) {
      val msg = s"""LSF support is NOT enabled. It can be enabled by defining loamstream.lsf section
                   |in the config file (${confFile}).""".stripMargin
        
      debug(msg)
    }
    
    (lsfRunnerOption, terminables)
  }
  
  private def unpack[A,B](o: Option[(A, Seq[B])]): (Option[A], Seq[B]) = o match {
    case Some((a, b)) => (Some(a), b)
    case None => (None, Nil)
  }

  private def makeCloudStorageClient(confFile: Option[Path], config: LoamConfig): Option[CloudStorageClient] = {
    
    val googleConfigAttempt: Try[GoogleCloudConfig] = config.googleConfig match {
      case Some(googleConfig) => Success(googleConfig)
      case None => Tries.failure(s"Missing or malformed 'loamstream.googlecloud' section in config file")
    }
    
    val gcsClientAttempt = for {
      googleConfig <- googleConfigAttempt
      gcsDriver <- GcsCloudStorageDriver.fromConfig(googleConfig)
    } yield {
      trace("Creating Google Cloud Storage Client...")
      
      GcsCloudStorageClient(gcsDriver)
    }

    if(gcsClientAttempt.isFailure) {
      val msg = s"""Job recording is turned off for outputs identified by URIs because
                    |Google Cloud Storage Client could not be created due to ${gcsClientAttempt.failed.get.getMessage}
                    |in the config file (${confFile})""".stripMargin
      debug(msg)
    }

    gcsClientAttempt.toOption
  }

  private def makeUgerChunkRunner(
      loamConfig: LoamConfig, 
      executionContext: ExecutionContext): Option[(DrmChunkRunner, Seq[Terminable])] = {
    
    for {
      ugerConfig <- loamConfig.ugerConfig
    } yield {
      debug("Creating Uger ChunkRunner...")

      import loamstream.model.execute.ExecuterHelpers._

      val sessionSource = QconfSessionSource.fromExecutable(ugerConfig)
      
      implicit val ec = executionContext
      
      val scheduler = ExecutionContextScheduler(executionContext)

      //TODO: Make configurable?
      val pollingFrequencyInHz = 0.1
      
      val poller = {
        QstatQacctPoller.fromExecutables(sessionSource, pollingFrequencyInHz, ugerConfig, scheduler = scheduler)
      }
      
      val jobMonitor = new JobMonitor(scheduler, poller, pollingFrequencyInHz)

      val jobSubmitter = QsubJobSubmitter.fromExecutable(sessionSource, ugerConfig, scheduler = scheduler)
      
      val accountingClient = QacctAccountingClient.useActualBinary(ugerConfig, scheduler)
      
      val jobKiller = QdelJobKiller.fromExecutable(sessionSource, ugerConfig, isSuccess = Set(0,1).contains)
      
      val ugerRunner = DrmChunkRunner(
          environmentType = EnvironmentType.Uger,
          pathBuilder = new UgerPathBuilder(UgerScriptBuilderParams(ugerConfig)),
          executionConfig = loamConfig.executionConfig, 
          drmConfig = ugerConfig, 
          jobSubmitter = jobSubmitter, 
          jobMonitor = jobMonitor,
          accountingClient = accountingClient,
          jobKiller = jobKiller)

      val handles = Seq(ugerRunner, sessionSource)

      (ugerRunner, handles)
    }
  }
  
  private def makeLsfChunkRunner(
      loamConfig: LoamConfig, 
      executionContext: ExecutionContext): Option[(DrmChunkRunner, Seq[Terminable])] = {
    
    for {
      lsfConfig <- loamConfig.lsfConfig
    } yield {
      debug("Creating LSF ChunkRunner...")

      import loamstream.model.execute.ExecuterHelpers._

      val scheduler = ExecutionContextScheduler(executionContext)
      
      implicit val ec = executionContext

      //TODO: Make configurable?
      val pollingFrequencyInHz = 0.1

      val poller = BjobsPoller.fromExecutable()
      
      val jobMonitor = new JobMonitor(scheduler, poller, pollingFrequencyInHz)

      val jobSubmitter = BsubJobSubmitter.fromExecutable(lsfConfig)

      val accountingClient = BacctAccountingClient.useActualBinary(lsfConfig, scheduler)
      
      val jobKiller = BkillJobKiller.fromExecutable(SessionSource.Noop, lsfConfig, isSuccess = ExitCodes.isSuccess)
      
      val lsfRunner = DrmChunkRunner(
          environmentType = EnvironmentType.Lsf,
          pathBuilder = LsfPathBuilder,
          executionConfig = loamConfig.executionConfig, 
          drmConfig = lsfConfig, 
          jobSubmitter = jobSubmitter, 
          jobMonitor = jobMonitor,
          accountingClient = accountingClient,
          jobKiller = jobKiller)

      val handles = Seq(lsfRunner)

      (lsfRunner, handles)
    }
  }
  
  private def loadConfig(confFileOpt: Option[Path]): Config = {
    def defaults: Config = ConfigFactory.load()

    confFileOpt match {
      case Some(confFile) => ConfigUtils.configFromFile(confFile).withFallback(defaults)
      case None           => ConfigUtils.allowSyspropOverrides(defaults)
    }
  }

  private[apps] def makeDaoFrom(dbDescriptor: DbDescriptor): LoamDao = {
    val dao = new SlickLoamDao(dbDescriptor)

    dao.createTables()

    dao
  }
  
  private[apps] def makeDefaultDb(loamConfig: LoamConfig): LoamDao = makeDefaultDbIn(loamConfig.executionConfig.dbDir)
  
  private[apps] def makeDefaultDbIn(dbDir: Path): LoamDao = {
    makeDaoFrom(DbDescriptor.onDiskHsqldbAt(dbDir, DbDescriptor.defaultDbName))
  }
  
  private[apps] final class TerminableExecuter(
      val delegate: Executer,
      toStop: Terminable*) extends Executer {

    override def execute(
        executable: Executable, 
        makeJobOracle: Executable => DirOracle[LJob])
       (implicit timeout: Duration = Duration.Inf): Map[LJob, Execution] = {
      
      delegate.execute(executable, makeJobOracle)(timeout)
    }

    override def jobFilter: JobFilter = delegate.jobFilter
    
    def shutdown(): Seq[Throwable] = {
      import Throwables.quietly
      
      for {
        terminable <- toStop :+ delegate
        e <- quietly("Error shutting down: ")(terminable.stop()) 
      } yield e
    }
  }
  
  private[apps] def defaultJobCanceller(protectedOutputsFile: Option[Path]): JobCanceler = {
    val protectedFilesJobCanceler: JobCanceler = {
      import ProtectsFilesJobCanceler.{empty, fromFile}
      
      protectedOutputsFile.map(fromFile).getOrElse(empty)
    }
    
    RequiresPresentInputsJobCanceler || protectedFilesJobCanceler
  }
  
  private[apps] def defaultJobFilter(dao: LoamDao, outputHashingStrategy: HashingStrategy): JobFilter = {
    RunsIfNoOutputsJobFilter || new DbBackedJobFilter(dao, outputHashingStrategy)
  }
}
