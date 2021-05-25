package aml4s.extraction.timeseries

import breeze.linalg._
import breeze.numerics._
import breeze.signal.fourierTr
import breeze.stats.median
import org.apache.commons.math3.stat.descriptive.moment.{Kurtosis, Skewness}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.Dataset

object TimeserieFeatureCalculators {

  /** * Calculates the number of crossings of ts on 0. A crossing is defined as two sequential
    * values where the first value is lower than 0 and the next is greater, or vice-versa.
    */
  def zeroCrossing[T](ts: DenseVector[Double]): Int = {
    val _sign = signum(ts)
    where(diff(_sign)).headOption.getOrElse(0)
  }

  /** Calculates the highest value of the time series ts.
    */
  def maximum(ts: DenseVector[Double]): Double = {
    max(ts)
  }

  /** Calculates the lowest value of the time series ts.
    */
  def minimum(ts: DenseVector[Double]): Double = {
    min(ts)
  }

  /** Returns the kurtosis of ts (Apache commons math implementation).
    */
  def kurtosis(ts: Array[Double]): Double = {
    new Kurtosis().evaluate(ts)
  }

  /** Returns the skewness of ts (Apache commons math implementation).
    */
  def skewness(ts: Array[Double]): Double = {
    new Skewness().evaluate(ts)
  }

  /** Returns the mean of ts
    */
  def mean(ts: DenseVector[Double]): Double = {
    breeze.stats.mean(ts)
  }

  /** Returns the variance of ts
    */
  def variance(ts: DenseVector[Double]): Double = {
    breeze.stats.variance(ts)
  }

  /** """ Boolean variable denoting if the distribution of x *looks symmetric*. This is the case if
    * .. math::
    * | mean(X)-median(X)| < r * (max(X)-min(X))
    *
    * @param ts
    *   : timeseries to be analyzed
    * @param percentages
    *   : array of percentage of the range to compare with
    */
  def symmetryLooking(ts: DenseVector[Double], percentages: Seq[Double]): Seq[Boolean] = {
    val meanMedianDifference = abs(mean(ts) - abs(median(ts)))
    val maxMinDifference     = maximum(ts) - minimum(ts)
    percentages.map(perc => meanMedianDifference < (perc * maxMinDifference))
  }

  /** Returns the discrete fourier transform of the ts. N.B it holds only the real part of each
    * complex number
    */
  def fftCoefficient(ts: DenseVector[Double]): Array[Double] = {
    fourierTr(ts).map(_.real).toArray
  }

}
