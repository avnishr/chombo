package org.chombo.spark.jobs

import java.util
import java.util.function.Consumer

import org.chombo.spark.common.JobConfiguration
import com.typesafe.config.Config
import main.scala.org.chombo.spark.common.SecondarySortScala
import org.apache.spark.SparkContext
import org.chombo.util.{SecondarySort, Tuple, Utility}
import org.chombo.spark.common._

object ScalaRecordSetBulkMutator extends JobConfiguration {

  var invalidRecordsOutputFile: Option[String] = None
  var fieldDelimRegex: String = ""
  var opCodeFieldOrdinal: Int = 0
  var deleteOpCode: String = ""
  var deletedRecFilePrefix: String = ""
  var temporalOrderingFieldFieldOrdinal: Int = 0
  var isTemporalOrderingFieldNumeric: Boolean = true
  var idFieldOrdinals: java.util.List[Integer] = new util.ArrayList[Integer]();

  def main(args: Array[String]) {

    val appName = "recordSetBulkMutator"

    val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)

    val config = createConfig(configFile)

    val sparkConf = createSparkConf(appName, config, false)
    val sparkCntxt = new SparkContext(sparkConf)
    val appConfig = config.getConfig(appName)

    readConfig(config)

    processFiles(inputPath, outputPath, sparkCntxt)

  }

  def readConfig(appConfig: Config) = {

    invalidRecordsOutputFile = getOptionalStringParam(appConfig, "invalid.records.output.file")
    fieldDelimRegex = getStringParamOrElse(appConfig, "field.delim.in", ",")

    opCodeFieldOrdinal = getIntParamOrElse(appConfig, "rsbm.op.code.field.ordinal", -1)
    deleteOpCode = getStringParamOrElse(appConfig, "rsbm.deleted.op.code", "D")
    if (opCodeFieldOrdinal < 0) {
      deletedRecFilePrefix = getMandatoryStringParam(appConfig, "rsbm.deleted.record.file.prefix")
      if (null == deletedRecFilePrefix) {
        throw new IllegalArgumentException("deleted data should either be in files with configured prefix in file name or op code field ordinal value should be provided ")
      }
      else {
        //val isDeletedDataFileSplit = spliFileName.startsWith(deletedRecFilePrefix)
      }
    }

    temporalOrderingFieldFieldOrdinal = getIntParamOrElse(appConfig, "rsbm.temporal.ordering.field.ordinal", -1)
    isTemporalOrderingFieldNumeric = getBooleanParamOrElse(appConfig, "rsbm.temporal.ordering.field.numeric", true)

    //    if (temporalOrderingFieldFieldOrdinal == -1) {
    //      val items: Array[String] = spliFileName.split("_")
    //      splitSequence = Long.parseLong(items(items.length - 1))
    //    }

    idFieldOrdinals = getMandatoryIntListParam(appConfig, "rsbm.id.field.ordinals")

  }


  def processFiles(inputPath: String, outputPath: String, sparkCntxt: SparkContext) = {

    /**
      * We would read a line and create a tuple of Tuples which would contain, key and action in key Tuple
      * while the value tuple will have the entire record
      */
    val rdd = sparkCntxt.textFile(inputPath).map(x => convertLineToTuple(x))

    /**
      * The above tuples now need to be grouped so that all the records with same id ordinals i.e, the records which have
      * key tuple with the first two fields same are grouped together.
      *
      * The partitioner should also be looking at the first two fields of the key tuple.
      */

    rdd.reduceByKey(SecondarySortScala.TuplePairPartitioner.class, )

  }

  def convertLineToTuple(line: String): (Tuple, Tuple) = {
    val items = line.split(",")
    val keyOut: Tuple = new Tuple()
    val valOut: Tuple = new Tuple()

    idFieldOrdinals.forEach(new Consumer[Integer] {
      override def accept(t: Integer): Unit = {
        keyOut.add(items(t))
      }
    })

    temporalOrderingFieldFieldOrdinal match {
      case -1 => println("No temporal ordering field provided")
      case _ => {
        if (isTemporalOrderingFieldNumeric) {
          keyOut.append(Long.unbox(items(temporalOrderingFieldFieldOrdinal)))
        }
        else {
          keyOut.append(items(temporalOrderingFieldFieldOrdinal))
        }
      }
    }

    opCodeFieldOrdinal match {
      case -1 => println("No opCodeField Ordinal for delete provided")
      case _ => {
        val record: String = Utility.join(items, 0, opCodeFieldOrdinal) + fieldDelimRegex + Utility.join(items, opCodeFieldOrdinal + 1, items.length)
        valOut.add(record)
      }
    }
    val opCode = items(opCodeFieldOrdinal)
    opCode match {
      case deleteOpCode => valOut.append(deleteOpCode)
      case _ => None
    }

    (keyOut, valOut)
  }

}
