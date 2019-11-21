package loamstream.apps

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import java.nio.file.Path

import loamstream.cli.Conf
import loamstream.cli.Intent
import loamstream.cli.Intent.RealRun
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.conf.ExecutionConfig
import loamstream.conf.LoamConfig
import loamstream.conf.PythonConfig
import loamstream.conf.RConfig
import loamstream.conf.UgerConfig

import loamstream.db.LoamDao
import loamstream.db.slick.DbDescriptor
import loamstream.db.slick.DbType
import loamstream.db.slick.SlickLoamDao

import loamstream.drm.AccountingClient
import loamstream.drm.Drmaa1Client
import loamstream.drm.DrmaaPoller
import loamstream.drm.DrmChunkRunner
import loamstream.drm.DrmSystem
import loamstream.drm.JobMonitor
import loamstream.drm.JobSubmitter
import loamstream.drm.Poller

import loamstream.drm.lsf.BjobsPoller
import loamstream.drm.lsf.BkillJobKiller
import loamstream.drm.lsf.BsubJobSubmitter
import loamstream.drm.lsf.LsfPathBuilder

import loamstream.drm.uger.QacctAccountingClient
import loamstream.drm.uger.UgerNativeSpecBuilder
import loamstream.drm.uger.UgerPathBuilder

import loamstream.googlecloud.CloudSdkDataProcWrapper
import loamstream.googlecloud.CloudStorageClient
import loamstream.googlecloud.GcsCloudStorageClient
import loamstream.googlecloud.GcsCloudStorageDriver
import loamstream.googlecloud.GoogleCloudConfig
import loamstream.googlecloud.GoogleCloudChunkRunner

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

import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob

import loamstream.util.ConfigUtils
import loamstream.util.ExecutionContexts
import loamstream.util.FileMonitor
import loamstream.util.Loggable
import loamstream.util.RxSchedulers
import loamstream.util.Terminable
import loamstream.util.Throwables
import loamstream.util.Tries

import scala.concurrent.duration.Duration
import scala.util.Try
import scala.util.Success
import loamstream.model.execute.ExecutionRecorder
import loamstream.model.execute.DbBackedExecutionRecorder
import loamstream.cli.JobFilterIntent
import loamstream.model.execute.ByNameJobFilter
import loamstream.drm.uger.UgerScriptBuilderParams
import loamstream.model.execute.RunsIfNoOutputsJobFilter
import loamstream.model.execute.RequiresPresentInputsJobCanceler
import loamstream.model.execute.JobCanceler
import loamstream.conf.Locations
import loamstream.drm.lsf.BacctAccountingClient
import loamstream.drm.DrmaaClient
import loamstream.model.execute.FileSystemExecutionRecorder
import loamstream.googlecloud.HailCtlDataProcClient
import loamstream.googlecloud.HailConfig
import loamstream.model.jobs.JobOracle
import scala.concurrent.ExecutionContext
import loamstream.conf.DrmConfig
import loamstream.drm.PathBuilder
import loamstream.conf.LsfConfig
import loamstream.util.Functions


/**
 * @author clint
 *         kyuksel
 * Nov 10, 2016
 */
trait AppWiring {
  def config: LoamConfig
  
  def dao: LoamDao

  def executer: Executer

  def cloudStorageClient: Option[CloudStorageClient]
  
  def jobFilter: JobFilter
  
  def executionRecorder: ExecutionRecorder
  
  def shutdown(): Iterable[Throwable]
  
  lazy val loamEngine: LoamEngine = LoamEngine(config, LoamCompiler.default, executer, cloudStorageClient)
  
  lazy val loamRunner: LoamRunner = LoamRunner(loamEngine)
}

object AppWiring extends Loggable {

  def loamConfigFrom(
      confFile: Option[Path], 
      drmSystemOpt: Option[DrmSystem], 
      shouldValidateGraph: Boolean): LoamConfig = {
    
    val typesafeConfig: Config = loadConfig(confFile)
      
    //TODO: Revisit .get
    val withoutDrmSystem = LoamConfig.fromConfig(typesafeConfig).get
    
    val withDrmSystem = withoutDrmSystem.copy(drmSystem = drmSystemOpt)
    
    val newCompilationConfig = withDrmSystem.compilationConfig.copy(shouldValidateGraph = shouldValidateGraph)
    
    withDrmSystem.copy(compilationConfig = newCompilationConfig)
  }
  
  def jobFilterForDryRun(intent: Intent.DryRun, makeDao: => LoamDao): JobFilter = {
    AppWiring.makeJobFilter(intent.jobFilterIntent, intent.hashingStrategy, makeDao)
  }
  
  def forRealRun(intent: Intent.RealRun, makeDao: => LoamDao): AppWiring = {
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
  
  private[AppWiring] def makeExecutionRecorder(getDao: => LoamDao): ExecutionRecorder = {
    FileSystemExecutionRecorder && (new DbBackedExecutionRecorder(getDao))
  }
  
  private[apps] def executerWindowLength(executionConfig: ExecutionConfig, intent: Intent.RealRun): Duration = {
    import scala.concurrent.duration._
    
    //NB: Stopgap hack: allow zipping through jobs quickly if we're running them by name.
    intent.jobFilterIntent match {
      case JobFilterIntent.AsByNameJobFilter(_) => 1.millisecond
      case _ => executionConfig.windowLength
    }
  }
  
  private final class DefaultAppWiring(
      intent: Intent.RealRun,
      makeDao: => LoamDao) extends AppWiring {
    
    override lazy val dao: LoamDao = makeDao
    
    override lazy val config: LoamConfig = loamConfigFrom(intent.confFile, intent.drmSystemOpt, intent.shouldValidate)
    
    override def executer: Executer = terminableExecuter

    override def shutdown(): Iterable[Throwable] = terminableExecuter.stop()

    override lazy val cloudStorageClient: Option[CloudStorageClient] = makeCloudStorageClient(intent.confFile, config)

    override lazy val jobFilter: JobFilter = makeJobFilter(intent.jobFilterIntent, intent.hashingStrategy, dao)
    
    override lazy val executionRecorder: ExecutionRecorder = makeExecutionRecorder(dao)
    
    private lazy val terminableExecuter: TerminableExecuter = {
      trace("Creating executer...")

      //TODO: Make this configurable?
      val threadPoolSize = 50

      val makeCompositeRunner: ChunkRunner.Constructor[CompositeChunkRunner] = makeChunkRunner(threadPoolSize)

      import loamstream.model.execute.ExecuterHelpers._
      import ExecutionContexts.threadPool

      val (executionContextWithThreadPool, threadPoolHandle) = threadPool(threadPoolSize)

      import scala.concurrent.duration._
      
      val windowLength = executerWindowLength(config.executionConfig, intent)
      
      import config.executionConfig.{ maxRunsPerJob, maxWaitTimeForOutputs, outputPollingFrequencyInHz }
      
      RxExecuter(
          config.executionConfig,
          makeCompositeRunner, 
          new FileMonitor(outputPollingFrequencyInHz, maxWaitTimeForOutputs),
          windowLength, 
          defaultJobCanceller,
          jobFilter, 
          executionRecorder,
          maxRunsPerJob,
          terminableComponents = Seq(threadPoolHandle))(executionContextWithThreadPool)
    }
    
    private def makeChunkRunner(threadPoolSize: Int): ChunkRunner.Constructor[CompositeChunkRunner] = {
      (shouldRestart, jobOracle) => {
        //TODO: Make the number of threads this uses configurable
        val numberOfCPUs = Runtime.getRuntime.availableProcessors
  
        val (localEC, localEcHandle) = ExecutionContexts.threadPool(numberOfCPUs * 2)
  
        val localRunner = AsyncLocalChunkRunner(config.executionConfig, jobOracle, shouldRestart)(localEC)
  
        val makeDrmRunnerOpt = drmChunkRunner(intent.confFile, config, threadPoolSize)(localEC)
        
        val drmRunnerOpt = makeDrmRunnerOpt.map(creationFn => creationFn(shouldRestart, jobOracle))
        
        val googleRunnerOpt = googleChunkRunner(intent.confFile, config.googleConfig, config.hailConfig, localRunner)
  
        CompositeChunkRunner(
            components = localRunner +: (googleRunnerOpt.toSeq ++ drmRunnerOpt),
            additionalTerminables = Seq(localEcHandle))
      }
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
      threadPoolSize: Int)(implicit ec: ExecutionContext): Option[ChunkRunner.Constructor[DrmChunkRunner]] = {

    loamConfig.drmSystem.flatMap {
      case DrmSystem.Uger => ugerChunkRunner(confFile, loamConfig, threadPoolSize)
      case DrmSystem.Lsf => lsfChunkRunner(confFile, loamConfig, threadPoolSize)
    }
  }
  
  private def ugerChunkRunner(
      confFile: Option[Path], 
      loamConfig: LoamConfig, 
      threadPoolSize: Int)(implicit ec: ExecutionContext): Option[ChunkRunner.Constructor[DrmChunkRunner]] = {
    
    //TODO: A better way to enable or disable Uger support; for now, this is purely expedient
    if(loamConfig.ugerConfig.isEmpty) {
      val msg = s"""Uger support is NOT enabled. It can be enabled by defining loamstream.uger section
                   |in the config file (${confFile}).""".stripMargin
        
      debug(msg)
    }
    
    makeUgerChunkRunner(loamConfig, threadPoolSize)
  }
  
  private def lsfChunkRunner(
      confFile: Option[Path], 
      loamConfig: LoamConfig, 
      threadPoolSize: Int)(implicit ec: ExecutionContext): Option[ChunkRunner.Constructor[DrmChunkRunner]] = {
    
    //TODO: A better way to enable or disable Uger support; for now, this is purely expedient
    if(loamConfig.lsfConfig.isEmpty) {
      val msg = s"""LSF support is NOT enabled. It can be enabled by defining loamstream.lsf section
                   |in the config file (${confFile}).""".stripMargin
        
      debug(msg)
    }
    
    makeLsfChunkRunner(loamConfig, threadPoolSize)
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

  private def makeDrmChunkRunner[C <: DrmConfig](
      threadPoolSize: Int,
      loamConfig: LoamConfig,
      configField: LoamConfig => Option[C],
      makePoller: C => Poller,
      makeJobSubmitter: C => JobSubmitter,
      makeAccountingClient: C => AccountingClient,
      makePathBuilder: C => PathBuilder,
      makeAdditionalTerminables: C => Iterable[Terminable] = (c: C) => Nil
      )(implicit ec: ExecutionContext): Option[ChunkRunner.Constructor[DrmChunkRunner]] = {
    
    for {
      drmConfig <- configField(loamConfig)
    } yield {
      (shouldRestart, jobOracle) => {
        debug("Creating Uger ChunkRunner...")
  
        val poller = makePoller(drmConfig)
  
        import loamstream.model.execute.ExecuterHelpers._
  
        val (scheduler, schedulerHandle) = RxSchedulers.backedByThreadPool(threadPoolSize)
  
        //TODO: Make configurable?
        val pollingFrequencyInHz = 0.1
        
        val jobMonitor = new JobMonitor(scheduler, poller, pollingFrequencyInHz)

        val jobSubmitter = makeJobSubmitter(drmConfig)
        
        val accountingClient = makeAccountingClient(drmConfig)
        
        val environmentType: EnvironmentType = drmConfig match {
          case _: UgerConfig => EnvironmentType.Uger
          case _: LsfConfig => EnvironmentType.Lsf
        }
        
        DrmChunkRunner(
            environmentType = environmentType,
            pathBuilder = makePathBuilder(drmConfig),
            executionConfig = loamConfig.executionConfig, 
            drmConfig = drmConfig, 
            jobSubmitter = jobSubmitter, 
            jobMonitor = jobMonitor,
            accountingClient = accountingClient,
            shouldRestart = shouldRestart,
            jobOracle = jobOracle,
            additionalTerminableComponents = Seq(schedulerHandle) ++ makeAdditionalTerminables(drmConfig))
      }
    }
  }
  
  private def makeUgerChunkRunner(
      loamConfig: LoamConfig, 
      threadPoolSize: Int)(implicit ec: ExecutionContext): Option[ChunkRunner.Constructor[DrmChunkRunner]] = {
    
    val makeDrmaaClient: UgerConfig => DrmaaClient = Functions.memoize { ugerConfig =>
      new Drmaa1Client(UgerNativeSpecBuilder(ugerConfig))
    }
    
    def makePoller(ugerConfig: UgerConfig): Poller = new DrmaaPoller(makeDrmaaClient(ugerConfig))
    
    def makeJobSubmitter(ugerConfig: UgerConfig): JobSubmitter = {
      JobSubmitter.Drmaa(makeDrmaaClient(ugerConfig), ugerConfig)
    }
    
    def makeAccountingClient(ugerConfig: UgerConfig): AccountingClient = QacctAccountingClient.useActualBinary(ugerConfig)
    
    def makePathBuilder(ugerConfig: UgerConfig) = new UgerPathBuilder(UgerScriptBuilderParams(ugerConfig))
    
    makeDrmChunkRunner(
      threadPoolSize, 
      loamConfig, 
      _.ugerConfig, 
      makePoller, 
      makeJobSubmitter, 
      makeAccountingClient, 
      makePathBuilder)
  }
  
  private def makeLsfChunkRunner(
      loamConfig: LoamConfig, 
      threadPoolSize: Int)(implicit ec: ExecutionContext): Option[ChunkRunner.Constructor[DrmChunkRunner]] = {
    
    def makeAdditionalTerminables(c: LsfConfig): Seq[Terminable] = Seq(Terminable { 
      BkillJobKiller.fromExecutable().killAllJobs()
    })
    
    makeDrmChunkRunner(
      threadPoolSize = threadPoolSize, 
      loamConfig = loamConfig, 
      configField = _.lsfConfig, 
      makePoller = (_: LsfConfig) => BjobsPoller.fromExecutable(), 
      makeJobSubmitter = BsubJobSubmitter.fromExecutable(_: LsfConfig), 
      makeAccountingClient = BacctAccountingClient.useActualBinary(_: LsfConfig), 
      makePathBuilder = (_: LsfConfig) => LsfPathBuilder,
      makeAdditionalTerminables = makeAdditionalTerminables)
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
  
  private[apps] def makeDefaultDb: LoamDao = makeDaoFrom(DbDescriptor.onDiskDefault)
  
  private[apps] def defaultJobCanceller: JobCanceler = RequiresPresentInputsJobCanceler
  
  private[apps] def defaultJobFilter(dao: LoamDao, outputHashingStrategy: HashingStrategy): JobFilter = {
    RunsIfNoOutputsJobFilter || new DbBackedJobFilter(dao, outputHashingStrategy)
  }
  
  type TerminableExecuter = Executer with Terminable
}
