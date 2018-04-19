/*
 * Copyright (c) 2018 Biopet
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package nl.biopet.tools.sparktest

import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import nl.biopet.utils.spark
import nl.biopet.utils.ngs.vcf
import nl.biopet.utils.ngs.intervals.BedRecordList
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.ml.linalg.Vectors

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.concurrent.Future

object SparkTest extends ToolCommand[Args] {

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")

    val sparkConf: SparkConf =
      new SparkConf(true).setMaster(cmdArgs.sparkMaster)
    implicit val sparkSession: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    implicit val sc: SparkContext = sparkSession.sparkContext
    logger.info(
      s"Context is up, see ${sparkSession.sparkContext.uiWebUrl.getOrElse("")}")

    val samples = sc.broadcast(vcf.getSampleIds(cmdArgs.inputFile).toArray)
    val regions = sc.parallelize(
      BedRecordList.fromReference(cmdArgs.reference).scatter(500000))
    val variants =
      regions.flatMap(r => vcf.loadRegions(cmdArgs.inputFile, r.iterator))
    val data = variants
      .mapPartitionsWithIndex {
        case (idx, it) =>
          val buffers: Map[String, ListBuffer[Int]] =
            samples.value.map(_ -> ListBuffer[Int]()).toMap
          it.foreach { record =>
            val alleles = record.getAlleles.indices
            buffers.foreach {
              case (sample, buf) =>
                val ad = Option(record.getGenotype(sample)).flatMap(x =>
                  Option(x.getAD)) match {
                  case Some(l) => l
                  case _       => Array.fill(alleles.size)(0)
                }
                alleles.foreach { i =>
                  buf.add(ad.lift(i).getOrElse(0))
                }
            }
          }
          buffers.toIterator.map {
            case (sample, list) => (idx, sample, list.toList)
          }
      }
      .groupBy(_._2)
      .map {
        case (sample, list) =>
          val vector = Vectors.dense(
            list.toArray.sortBy(_._1).flatMap(_._3).map(_.toDouble))
          sample -> vector
      }
      .toDF("sample", "features")
    if (cmdArgs.withCache) {
      data.cache()
      Future(data.count())
    }

    val bkm = new BisectingKMeans()
      .setK(5)
      .setSeed(12345)

    val model = bkm.fit(data)

    logger.info("Done")
  }

  def argsParser: ArgsParser = new ArgsParser(this)

  def emptyArgs = Args()

  def descriptionText: String = Array.fill(25)("a").mkString(" ")

  def manualText: String = Array.fill(25)("a").mkString(" ")

  def exampleText: String = Array.fill(25)("a").mkString(" ")

}
