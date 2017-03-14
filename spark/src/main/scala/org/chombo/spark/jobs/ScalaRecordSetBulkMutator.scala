package org.chombo.spark.jobs

import java.util.function.Consumer

import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.chombo.spark.common.JobConfiguration
import org.chombo.util.{Tuple, Utility}

object ScalaRecordSetBulkMutator extends JobConfiguration {

  var invalidRecordsOutputFile: Option[String] = None
  var fieldDelimRegex: String = ""
  var opCodeFieldOrdinal: Int = 0
  var deleteOpCode: String = ""
  var deletedRecFilePrefix: String = ""
  var temporalOrderingFieldFieldOrdinal: Int = 0
  var isTemporalOrderingFieldNumeric: Boolean = true
  var idFieldOrdinals: java.util.List[Integer] = new java.util.ArrayList[Integer]();

  def main(args: Array[String]) {

    val appName = "recordSetBulkMutator"

    val Array(inputPath: String, outputPath: String, configFile: String) = getCommandLineArgs(args, 3)

    val config = createConfig(configFile)

    val sparkConf = createSparkConf(appName, config, false)
    val sparkCntxt = new SparkContext(sparkConf)
    val appConfig = config.getConfig(appName)

    readConfig(config)

    processData(inputPath, outputPath, sparkCntxt)

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

  def processData(inputPath: String, outputPath: String, sparkCntxt: SparkContext) = {

    /**
      * We would read a line and create a tuple of Tuples which would contain, key and action in key Tuple
      * while the value tuple will have the entire record
      */
    val rdd: RDD[(Tuple, Tuple)] = sparkCntxt.textFile(inputPath).map(x => convertLineToTuple(x))

    /**
      * The reduce operation will filter out irrelevant records
      */
    val reducedRDD : RDD[(Tuple, Tuple)] = rdd.reduceByKey( (u,v) => removeRedundantData(u,v))

    val filteredRDD = reducedRDD.filter( x  => x._2.getString(3).equalsIgnoreCase(deleteOpCode))

    filteredRDD.saveAsTextFile(outputPath)

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
          valOut.append(Long.unbox(items(temporalOrderingFieldFieldOrdinal)))
        }
        else {
          valOut.append(items(temporalOrderingFieldFieldOrdinal))
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

  def removeRedundantData(u: Tuple, v: Tuple) : Tuple ={

    // there might be a delete operation if the tuple has three fields
    var retVal : Option[Tuple] = None

    retVal = u.getSize() match  {

      case 3 =>  Some(u)
      case _ => {
          v.getSize() match  {

            case 3 => Some(v)
            case _ => if ( u.getLong(0) >= v.getLong(0)) Some(u) else Some(v)
          }
      }
    }
    retVal.get
  }
}
