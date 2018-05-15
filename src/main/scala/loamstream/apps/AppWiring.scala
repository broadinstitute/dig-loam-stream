package loamstream.apps

import com.typesafe.config.Config
import loamstream.db.slick.DbDescriptor
import loamstream.db.LoamDao
import loamstream.model.execute.ChunkRunner
import loamstream.model.execute.Executer
import loamstream.cli.Conf
import com.typesafe.config.ConfigFactory
import loamstream.db.slick.SlickLoamDao
import loamstream.db.slick.DbType
import loamstream.conf.{LoamConfig, PythonConfig, RConfig, UgerConfig}
import loamstream.util.Loggable
import loamstream.drm.Poller
import loamstream.util.RxSchedulers
import loamstream.drm.JobMonitor
import loamstream.model.execute.RxExecuter
import loamstream.model.execute.Executable

import scala.concurrent.duration.Duration
import loamstream.model.jobs.{Execution, LJob}
import loamstream.model.execute.DbBackedJobFilter
import loamstream.model.execute.JobFilter
import loamstream.util.Terminable
import loamstream.model.execute.AsyncLocalChunkRunner
import loamstream.model.execute.CompositeChunkRunner
import loamstream.util.ExecutionContexts
import loamstream.googlecloud._
import loamstream.util.Throwables

import scala.util.Try
import loamstream.drm.AccountingClient
import loamstream.drm.Drmaa1Client
import loamstream.drm.DrmClient
import loamstream.conf.ExecutionConfig
import loamstream.util.ConfigUtils
import loamstream.util.Tries
import loamstream.drm.JobSubmitter
import loamstream.model.execute.HashingStrategy
import loamstream.cli.Intent
import loamstream.cli.Intent.RealRun
import java.nio.file.Path
import scala.util.Success
import loamstream.util.FileMonitor
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.drm.uger.QacctAccountingClient
import loamstream.drm.DrmaaPoller
import loamstream.drm.uger.UgerResourceUsageExtractor
import loamstream.drm.uger.UgerNativeSpecBuilder
import loamstream.drm.uger.DrmChunkRunner
import loamstream.drm.uger.UgerPathBuilder
import loamstream.model.execute.EnvironmentType


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

  def shutdown(): Seq[Throwable]
  
  lazy val loamEngine: LoamEngine = LoamEngine(config, LoamCompiler.default, executer, cloudStorageClient)
  
  lazy val loamRunner: LoamRunner = LoamRunner(loamEngine)
}

object AppWiring extends Loggable {

  def daoForOutputLookup(intent: Intent.LookupOutput): LoamDao = makeDefaultDb

  def loamConfigFrom(confFile: Option[Path]): LoamConfig = {
    val typesafeConfig: Config = loadConfig(confFile)
      
    //NB: .get is safe here, since we know LoamConfig.fromConfig won't return Failure
    //TODO: Revisit this
    LoamConfig.fromConfig(typesafeConfig).get
  }
  
  def jobFilterForDryRun(intent: Intent.DryRun, makeDao: => LoamDao): JobFilter = {
    makeJobFilter(intent.shouldRunEverything, intent.hashingStrategy, makeDao)
  }
  
  def forRealRun(intent: Intent.RealRun, makeDao: => LoamDao): AppWiring = {
    new DefaultAppWiring(
        confFile = intent.confFile,
        makeDao = makeDao,
        shouldRunEverything = intent.shouldRunEverything, 
        hashingStrategy = intent.hashingStrategy)
  }

  private[AppWiring] def makeJobFilter(
      shouldRunEverything: Boolean,
      hashingStrategy: HashingStrategy,
      dao: => LoamDao): JobFilter = {
    
    if (shouldRunEverything) { JobFilter.RunEverything }
    else { new DbBackedJobFilter(dao, hashingStrategy) }
  }  
  
  private final class DefaultAppWiring(
      confFile: Option[Path],
      makeDao: => LoamDao,
      shouldRunEverything: Boolean,
      hashingStrategy: HashingStrategy) extends AppWiring {
    
    override lazy val dao: LoamDao = makeDao
    
    override lazy val config: LoamConfig = loamConfigFrom(confFile)
    
    override def executer: Executer = terminableExecuter

    override def shutdown(): Seq[Throwable] = terminableExecuter.shutdown()

    override lazy val cloudStorageClient: Option[CloudStorageClient] = makeCloudStorageClient(confFile, config)

    override lazy val jobFilter: JobFilter = makeJobFilter(shouldRunEverything, hashingStrategy, dao)
    
    private lazy val terminableExecuter: TerminableExecuter = {
      trace("Creating executer...")

      //TODO: Make this configurable?
      val threadPoolSize = 50

      val (compositeRunner: ChunkRunner, runnerHandles: Seq[Terminable]) = makeChunkRunner(threadPoolSize)

      import loamstream.model.execute.ExecuterHelpers._
      import ExecutionContexts.threadPool

      val (executionContextWithThreadPool, threadPoolHandle) = threadPool(threadPoolSize)

      import scala.concurrent.duration._
      
      val windowLength = 30.seconds
      
      import config.executionConfig.{ maxRunsPerJob, maxWaitTimeForOutputs, outputPollingFrequencyInHz }
      
      val rxExecuter = {
        RxExecuter(
            compositeRunner, 
            new FileMonitor(outputPollingFrequencyInHz, maxWaitTimeForOutputs),
            windowLength, 
            jobFilter, 
            maxRunsPerJob)(executionContextWithThreadPool)
      }

      val handles: Seq[Terminable] = threadPoolHandle +: runnerHandles 

      new TerminableExecuter(rxExecuter, handles: _*)
    }
    
    private def makeChunkRunner(threadPoolSize: Int): (ChunkRunner, Seq[Terminable]) = {
      //TODO: Make the number of threads this uses configurable
      val numberOfCPUs = Runtime.getRuntime.availableProcessors

      val (localEC, localEcHandle) = ExecutionContexts.threadPool(numberOfCPUs)

      val localRunner = AsyncLocalChunkRunner(config.executionConfig)(localEC)

      val (ugerRunner, ugerRunnerHandles) = ugerChunkRunner(confFile, config, threadPoolSize)

      val googleRunner = googleChunkRunner(confFile, config.googleConfig, localRunner)

      val compositeRunner = CompositeChunkRunner(localRunner +: (ugerRunner.toSeq ++ googleRunner))
      
      val toBeStopped = compositeRunner +: localEcHandle +: (ugerRunnerHandles ++ compositeRunner.components)    
      
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
      delegate: ChunkRunner): Option[GoogleCloudChunkRunner] = {
    
    //TODO: A better way to enable or disable Google support; for now, this is purely expedient
    
    def noGoogleConfig: Option[GoogleCloudChunkRunner] = {
      debug("Google Cloud support NOT enabled due to missing 'loamstream.googlecloud' section in the config file")
        
      None
    }
    
    def googleConfigPresent(googleConfig: GoogleCloudConfig): Option[GoogleCloudChunkRunner] = {
      val clientAttempt = CloudSdkDataProcClient.fromConfig(googleConfig)
    
      val runnerAttempt: Try[GoogleCloudChunkRunner] = {
        for {
          client <- CloudSdkDataProcClient.fromConfig(googleConfig)
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
    
    googleConfigOpt.fold(noGoogleConfig)(googleConfigPresent)
  }
  
  private def ugerChunkRunner(
      confFile: Option[Path], 
      loamConfig: LoamConfig, 
      threadPoolSize: Int): (Option[DrmChunkRunner], Seq[Terminable]) = {
    
    val result @ (ugerRunnerOption, _) = unpack(makeUgerChunkRunner(loamConfig, threadPoolSize))

    //TODO: A better way to enable or disable Uger support; for now, this is purely expedient
    if(ugerRunnerOption.isEmpty) {
      val msg = s"""Uger support is NOT enabled. It can be enabled by defining loamstream.uger section
                   |in the config file (${confFile}).""".stripMargin
        
      debug(msg)
    }
    
    result
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
      gcsDriver <- GcsDriver.fromConfig(googleConfig)
    } yield {
      trace("Creating Google Cloud Storage Client...")
      
      GcsClient(gcsDriver)
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
      threadPoolSize: Int): Option[(DrmChunkRunner, Seq[Terminable])] = {
    
    for {
      ugerConfig <- loamConfig.ugerConfig
    } yield {
      debug("Creating Uger ChunkRunner...")

      val ugerClient = makeUgerClient

      import loamstream.model.execute.ExecuterHelpers._

      val poller = new DrmaaPoller(ugerClient)

      val (scheduler, schedulerHandle) = RxSchedulers.backedByThreadPool(threadPoolSize)

      val ugerRunner = {
        //TODO: Make configurable?
        val pollingFrequencyInHz = 0.1
        
        val jobMonitor = new JobMonitor(scheduler, poller, pollingFrequencyInHz)

        val jobSubmitter = JobSubmitter.Drmaa(ugerClient, ugerConfig)
        
        DrmChunkRunner(
            environmentType = EnvironmentType.Uger,
            pathBuilder = UgerPathBuilder,
            executionConfig = loamConfig.executionConfig, 
            drmConfig = ugerConfig, 
            jobSubmitter = jobSubmitter, 
            jobMonitor = jobMonitor)
      }

      val handles = Seq(schedulerHandle, ugerRunner)

      (ugerRunner, handles)
    }
  }
  
  private def makeUgerClient: DrmClient = {
    val drmaa1Client = new Drmaa1Client(UgerResourceUsageExtractor, UgerNativeSpecBuilder)
    
    new DrmClient(drmaa1Client, QacctAccountingClient.useActualBinary())
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
  
  private[apps] def makeInMemoryDb: LoamDao = makeDaoFrom(DbDescriptor.inMemory)
  
  private[apps] final class TerminableExecuter(
      val delegate: Executer,
      toStop: Terminable*) extends Executer {

    override def execute(executable: Executable)(implicit timeout: Duration = Duration.Inf): Map[LJob, Execution] = {
      delegate.execute(executable)(timeout)
    }

    override def jobFilter: JobFilter = delegate.jobFilter
    
    def shutdown(): Seq[Throwable] = {
      import Throwables.quietly
      
      for {
        terminable <- toStop
        e <- quietly("Error shutting down: ")(terminable.stop()) 
      } yield e
    }
  }
}
