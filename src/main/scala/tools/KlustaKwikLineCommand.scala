package tools

/**
  * LoamStream
  * Created by oliverr on 3/15/2016.
  */
object KlustaKwikLineCommand extends LineCommand {

  override def name: String = "klustakwik"

  def klustaKwik(fileBaseVal: String, elecNoVal: Int): CommandLine =
    KlustaKwikLineCommand + fileBase(fileBaseVal) + elecNo(elecNoVal)

  val fileBase = UnkeyedValueParam.Builder[String]("FileBase")
  val elecNo = UnkeyedValueParam.Builder[Int]("ElecNo")
  val useFeatures = DashValueParam.Builder[String]("UseFeatures")
  val dropLastNFeatures = DashValueParam.Builder[Int]("DropLastNFeatures")
  val useDistributional = DashValueParam.Builder[Int]("UseDistributional")
  val maskStarts = DashValueParam.Builder[Int]("MaskStarts")
  val minClusters = DashValueParam.Builder[Int]("MinClusters")
  val maxClusters = DashValueParam.Builder[Int]("MaxClusters")
  val maxPossibleClusters = DashValueParam.Builder[Int]("MaxPossibleClusters")
  val nStarts = DashValueParam.Builder[Int]("nStarts")
  val startCluFile = DashSwitchParam("StartCluFile")
  val splitEvery = DashValueParam.Builder[Int]("SplitEvery")
  val splitFirst = DashValueParam.Builder[Int]("SplitFirst")
  val penaltyK = DashValueParam.Builder[Double]("PenaltyK")
  val penaltyKLogN = DashValueParam.Builder[Double]("PenaltyKLogN")
  val subset = DashValueParam.Builder[Int]("Subset")
  val fullStepEvery = DashValueParam.Builder[Int]("FullStepEvery")
  val maxIter = DashValueParam.Builder[Int]("MaxIter")
  val randomSeed = DashValueParam.Builder[Int]("RandomSeed")
  val debug = DashValueParam.Builder[Int]("Debug")
  val splitInfo = DashValueParam.Builder[Int]("SplitInfo")
  val verbose = DashValueParam.Builder[Int]("Verbose")
  val distDump = DashValueParam.Builder[Int]("DistDump")
  val distThresh = DashValueParam.Builder[Double]("DistThresh")
  val changedThresh = DashValueParam.Builder[Double]("ChangedThresh")
  val log = DashValueParam.Builder[Int]("Log")
  val screen = DashValueParam.Builder[Int]("Screen")
  val priorPoint = DashValueParam.Builder[Int]("PriorPoint")
  val saveSorted = DashValueParam.Builder[Int]("SaveSorted")
  val saveCovarianceMeans = DashValueParam.Builder[Int]("SaveCovarianceMeans")
  val useMaskedInitialConditions = DashValueParam.Builder[Int]("UseMaskedInitialConditions")
  val assignToFirstClosestMask = DashValueParam.Builder[Int]("AssignToFirstClosestMask")
  val help = DashValueParam.Builder[Int]("help")
}
