package main.scala.org.chombo.spark.jobs

import org.chombo.spark.common.JobConfiguration
import org.chombo.spark.etl.DataValidator._

/**
  * Created by avi on 3/12/2017.
  */
object ScalaRecordSetBulkMutator extends JobConfiguration {

  def main(args: Array[String]) {

    val appName = "recordSetBulkMutator"
    val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)

    println(inputPath)
    println(outputPath)


  }

}
