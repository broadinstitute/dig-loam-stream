package loamstream.loam.intake

import org.scalatest.FunSuite

/**
 * @author clint
 * 16 Mar, 2021
 */
final class StatsTest extends FunSuite {
  ignore("effectiveN") {
    fail("TODO")
  }
  
  test("qnorm") {
    val pvalues = Seq(
      Double.MinPositiveValue,
      Double.MinPositiveValue,
      0.8171747564107117,
      0.6848577837070313,
      0.6464206666408658,
      0.14061842134410996,
      0.9099152863068104)
      
    val betas = Seq(
      -10.0,
      10.0,
      7.1522756890618435,
      2.80925706776019,
      0.13863301215970236,
      6.719806100369839,
      6.107730873922039)
      
    val pythonResults = Seq(
        0.0,
        0.0,
        30.938088532482876,
        6.922024998792508,
        0.30220368810665155,
        4.560463262129231,
        53.9812591807065)
      
    val expectations: Seq[((Double, Double), Double)] = betas.zip(pvalues).zip(pythonResults)
        
    val epsilon = 1e-8
    
    def withinEpsilon(a: Double, b: Double): Boolean = scala.math.abs(a - b) < epsilon
    
    for {
      ((beta, pvalue), expected) <- expectations
    } {
      assert(withinEpsilon(Stats.qnorm(beta, pvalue), expected))
    }
    
    for {
      beta <- betas
    } {
      assert(Stats.qnorm(beta, pvalue = 1.0) === Double.NegativeInfinity)
    }
  }
}