package tools

import scala.util.Random

/**
  * LoamStream
  * Created by oliverr on 3/16/2016.
  */
object KlustaKwikMockDataGenerator {
  val random = new Random()

  def generate(nSamples: Int, nPcas: Int, pcaMin: Double, pcaMax: Double,
               random: Random = this.random): Seq[Seq[Double]] = {
    Seq.fill(nSamples, nPcas)(pcaMin + (pcaMax - pcaMin) * random.nextDouble)
  }
}
