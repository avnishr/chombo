package org.chombo.spark.jobs

import org.chombo.spark.common.JobConfiguration
import org.apache.spark.SparkContext

/**
  * Created by avi on 3/12/2017.
  */
object ScalaRecordSetBulkMutator extends JobConfiguration {

  def main(args: Array[String]) {

    val appName = "recordSetBulkMutator"

    val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)

    val config = createConfig(configFile)

    val sparkConf = createSparkConf(appName, config, false)
    val sparkCntxt = new SparkContext(sparkConf)
    val appConfig = config.getConfig(appName)

    val invalidRecordsOutputFile = getOptionalStringParam(appConfig, "invalid.records.output.file")
    val fieldDelimIn = getStringParamOrElse(appConfig, "field.delim.in", ",")
    val fieldDelimOut = getStringParamOrElse(appConfig, "field.delim.out", ",")

    println("The spark configuration for invalid records is " + invalidRecordsOutputFile)
    println("The spark configuration for field delim in " + invalidRecordsOutputFile)
    println("The spark configuration for field delim out " + invalidRecordsOutputFile)


  }

}
