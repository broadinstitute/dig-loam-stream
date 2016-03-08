package tools

import utils.Loggable

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 3/8/16.
  */
object PcaWeightsReaderApp extends App with Loggable {
  debug("" + PcaWeightsReader.weightsFilePath.map(PcaWeightsReader.read))
}
