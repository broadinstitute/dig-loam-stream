package tools

import org.scalatest.FunSuite
import tools.klusta.KlustaKwikLineCommand.{assignToFirstClosestMask, klustaKwik, maxClusters, maxPossibleClusters,
    minClusters, penaltyK, penaltyKLogN, useDistributional, useFeatures, useMaskedInitialConditions}

/**
  * LoamStream
  * Created by oliverr on 3/15/2016.
  */
class KlustaKwikLineCommandTest extends FunSuite {
  test("Constructing KlustaKwik command line works as expected.") {
    val desiredCommandLine = "klustakwik recording 4 -UseDistributional 1 -UseMaskedInitialConditions 1 " +
      "-AssignToFirstClosestMask 1 -MaxPossibleClusters 500 -MinClusters 300 -MaxClusters 300 -PenaltyK 1.0 " +
      "-PenaltyKLogN 0.0 " +
      "-UseFeatures 111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111110"
    // scalastyle:off magic.number
    val command = klustaKwik("recording", 4) + useDistributional(1) + useMaskedInitialConditions(1) +
      assignToFirstClosestMask(1) + maxPossibleClusters(500) + minClusters(300) + maxClusters(300) + penaltyK(1.0) +
      penaltyKLogN(0.0) +
      useFeatures("111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111110")
    // scalastyle:on magic.number
    val constructedCommandLine = command.commandLine
    assert(constructedCommandLine === desiredCommandLine)
  }
}
