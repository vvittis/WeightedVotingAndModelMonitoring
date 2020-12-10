import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.util.AccumulatorV2

object Job2_AggregateResults {

  // Case class
  //case class Result(keyTuple:Double,res:Int,timestamp: Timestamp,idT:Int)

  case class Result(keyTuple: Int, res: Int, label: Int, purposeId: Int, idT: Int, weightTree: Double) extends Serializable

  case class GroupedResult(keyTuple: Int, purposeId: Int, fd: String) extends Serializable


  // Custom aggregation
  case class IntermediateStateAggr(var countF: Int, var countNF: Int)

  object MyAggregate extends Aggregator[Int, IntermediateStateAggr, String] {

    def zero: IntermediateStateAggr = IntermediateStateAggr(0, 0)

    def reduce(isa: IntermediateStateAggr, a: Int): IntermediateStateAggr = {
      //a.get(1)
      if (a == 0) {
        isa.countNF += 1
      }
      else {
        isa.countF += 1
      }
      isa
    }

    def merge(b1: IntermediateStateAggr, b2: IntermediateStateAggr): IntermediateStateAggr = {
      IntermediateStateAggr(b1.countF + b2.countF, b1.countNF + b2.countNF)
    }

    def finish(reduction: IntermediateStateAggr): String = {
      if (reduction.countF > reduction.countNF) {
        "Fraud"
      }
      else {
        "Not-Fraud"
      }
    }

    // Transform the output of the reduction
    def bufferEncoder: Encoder[IntermediateStateAggr] = Encoders.product

    def outputEncoder: Encoder[String] = Encoders.STRING

  }


  // Weighted Voting - Aggregator
  case class InputWeightedVote(vote: Int, trueLabel: Int, weight: Double)

  case class IntermediateStateWeightedVote(var weightedSum: Double, var trueLabel: Int)

  object WeightedVoting extends Aggregator[InputWeightedVote, IntermediateStateWeightedVote, String] {

    def zero: IntermediateStateWeightedVote = IntermediateStateWeightedVote(0.0, -1)

    def reduce(iswv: IntermediateStateWeightedVote, a: InputWeightedVote): IntermediateStateWeightedVote = {
      iswv.weightedSum += a.weight * a.vote
      iswv.trueLabel = a.trueLabel
      iswv
    }

    def merge(b1: IntermediateStateWeightedVote, b2: IntermediateStateWeightedVote): IntermediateStateWeightedVote = {

      var label = -1

      // Predicting tuple
      if (b1.trueLabel == -1 && b2.trueLabel == -1) {
        label = b1.trueLabel
      }
      // Testing tuple
      else if (b1.trueLabel != -1 && b2.trueLabel == -1) {
        label = b1.trueLabel
      }
      else if (b2.trueLabel != -1 && b1.trueLabel == -1) {
        label = b2.trueLabel
      }
      else {
        label = b1.trueLabel
      }

      IntermediateStateWeightedVote(b1.weightedSum + b2.weightedSum, label)
    }

    def finish(reduction: IntermediateStateWeightedVote): String = {
      if (reduction.weightedSum > 0.4 && reduction.trueLabel == -1) {
        println(reduction.weightedSum)
        "Fraud"
      }
      else if (reduction.trueLabel == -1) {
        "Not-Fraud"
      }
      else if (reduction.weightedSum > 0.4 && reduction.trueLabel != -1) {
        println(reduction.weightedSum)
        "Fraud".concat(",").concat(reduction.trueLabel.toString)
      }
      else {
        "Not-Fraud".concat(",").concat(reduction.trueLabel.toString)
      }
    }

    // Transform the output of the reduction
    def bufferEncoder: Encoder[IntermediateStateWeightedVote] = Encoders.product

    def outputEncoder: Encoder[String] = Encoders.STRING

  }


  // Confusion matrix - Accumulator
  class InputAccum(var accuracy: Double, var sensitivity: Double, var specificity: Double, var precision: Double, var f1_score: Double, var true_positive: Int, var true_negative: Int, var false_positive: Int, var false_negative: Int) extends Serializable {

    def add(inputAccum: InputAccum): InputAccum = {
      true_positive += inputAccum.true_positive
      true_negative += inputAccum.true_negative
      false_positive += inputAccum.false_positive
      false_negative += inputAccum.false_negative

      // Accuracy = (TP+TN)/ (TP + TN + FP + FN)
      accuracy += (true_positive + true_negative).toDouble / (true_positive + true_negative + false_negative + false_positive)
      // Sensitivity = TP / (TP + FN)
      sensitivity += true_positive.toDouble / (true_positive + false_negative)
      // Specificity = TN / (TN + FP)
      specificity += true_negative.toDouble / (true_negative + false_positive)
      // Precision   = TP / (TP + FP)
      precision += true_positive.toDouble / (true_positive + false_positive)
      // F1-score    = 2*(Precision * Sensitivity)/(Precision + Sensitivity)
      f1_score += 2 * precision * sensitivity / (precision + sensitivity)
      this
    }

  }

  object MyAccumulator extends AccumulatorV2[String, InputAccum] {

    private val myInput: InputAccum = new InputAccum(0, 0, 0, 0, 0, 0, 0, 0, 0)

    def isZero: Boolean = {
      myInput.true_positive == 0
    }

    def copy(): AccumulatorV2[String, InputAccum] = {
      MyAccumulator
    }

    def reset(): Unit = {
      myInput.accuracy = 0.0
      myInput.sensitivity = 0.0
      myInput.specificity = 0.0
      myInput.precision = 0.0
      myInput.f1_score = 0.0
      myInput.true_positive = 0
      myInput.true_negative = 0
      myInput.false_positive = 0
      myInput.false_negative = 0
    }

    def add(v: String): Unit = {

      if (v.split(",")(0).equals("Fraud") && v.split(",")(1).toInt == 1) {
        println("true_positive")
        myInput.true_positive = myInput.true_positive + 1
      }
      else if (v.split(",")(0).equals("Fraud") && v.split(",")(1).toInt == 0) {
        //        println("false_positive")
        myInput.false_positive = myInput.false_positive + 1
      }
      else if (v.split(",")(0).equals("Not-Fraud") && v.split(",")(1).toInt == 1) {
        println("false_negative")
        myInput.false_negative = myInput.false_negative + 1
      }
      else if (v.split(",")(0).equals("Not-Fraud") && v.split(",")(1).toInt == 0) {
        //        println("true_negative")
        myInput.true_negative = myInput.true_negative + 1
      }


      println("fp:" + myInput.false_positive + " ,tn:" + myInput.true_negative + " ,tp:" + myInput.true_positive + " ,fn:" + myInput.false_negative)
      // Accuracy = (TP+TN)/ (TP + TN + FP + FN)
      myInput.accuracy = (myInput.true_positive + myInput.true_negative).toDouble / (myInput.true_positive + myInput.true_negative + myInput.false_negative + myInput.false_positive)
      // Sensitivity = TP / (TP + FN)
      myInput.sensitivity = myInput.true_positive.toDouble / (myInput.true_positive + myInput.false_negative).toDouble
      // Specificity = TN / (TN + FP)
      myInput.specificity = myInput.true_negative.toDouble / (myInput.true_negative + myInput.false_positive).toDouble
      // Precision   = TP / (TP + FP)
      myInput.precision = myInput.true_positive.toDouble / (myInput.true_positive + myInput.false_positive).toDouble
      // F1-score    = 2*(Precision * Sensitivity)/(Precision + Sensitivity)
      myInput.f1_score = 2 * myInput.precision * myInput.sensitivity / (myInput.precision + myInput.sensitivity)
      println("On add input.acc " + myInput.accuracy)
    }

    def merge(other: AccumulatorV2[String, InputAccum]): Unit = {
      //println("On merge other.acc "+other.value.accuracy+" ,fp:"+other.value.false_positive+" ,tn:"+other.value.true_negative+" ,tp:"+other.value.true_positive+" ,fn:"+other.value.false_negative)
      //myInput.add(other.value)
      //println("On merge input.acc "+myInput.accuracy+" ,fp:"+myInput.false_positive+" ,tn:"+myInput.true_negative+" ,tp:"+myInput.true_positive+" ,fn:"+myInput.false_negative)
    }

    def value: InputAccum = {
      myInput
    }

  }


  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface(checkpoint iff stateful operator is needed)
    val spark = SparkSession.builder()
      .appName("SparkStructuredStreamingExample")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "checkpoint_saves/")
      .getOrCreate()


    val rawData = spark.readStream
//      .text("/user/vvittis/results")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testSink")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
    //option("minPartitions",14)

    // Result : keyTuple,res,label,purposeId,idT,weightTree

    import spark.implicits._
    val structuredData = rawData.map {
      line: Row =>
        val fields = line.getString(0).trim.split(",")
        val res: Result = Result(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toInt, fields(4).toInt, fields(5).toDouble)
        res
    }

    // Print schema
    structuredData.printSchema
    //    case class Result(keyTuple: Int, res: Int, label:Int, purposeId:Int, idT: Int, weightTree:Double) extends Serializable

    // val myUdaf = udaf(MyAggregate) , User defined aggregation function(extends Aggregator)
    val myWV = udaf(WeightedVoting) // User defined aggregation function(extends Aggregator)
    val groupedResult = structuredData.groupBy($"keyTuple", $"purposeId").agg(myWV($"res", $"label", $"weightTree").name("fd")).as[GroupedResult]

    // Predicted tuples
    // Firstly,filter in order to select only predicted tuples and after dumping the results
    //    val predictedTuples = groupedResult.filter(x => x.purposeId != -5)
    //      .writeStream
    //      .outputMode("update")
    //      .trigger(Trigger.ProcessingTime("4 seconds"))
    //      .format("console")
    //      .queryName("Results")
    //      .start()

    // Testing tuples
    // Firstly,filter in order to select only testing tuples and after calculate the confusion matrix for RF
    spark.sparkContext.register(MyAccumulator, "Confusion_Matrix")
    val accum = MyAccumulator
    val testingTuples = groupedResult.filter(x => x.purposeId != -10).map(x => {
      accum.add(x.fd);
      x
    })
      .writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("4 seconds"))
      .format("console")
      .queryName("Metrics")
      .start()


    println(accum.value)
    println(" accum.acc " + accum.value.accuracy + " ,fp:" + accum.value.false_positive + " ,tn:" + accum.value.true_negative + " ,tp:" + accum.value.true_positive + " ,fn:" + accum.value.false_negative)

    // Start the streaming query, dumping results to the console. Use "complete" output mode because we are aggregating( instead of "append" mode)
    /*val query = groupedResult.writeStream
                .outputMode("update")
                .trigger(Trigger.ProcessingTime("4 seconds"))
                .format("console")
                .queryName("Metrics-Results")
                .start()*/

    //outputMode("complete")
    //milliseconds

    // Keep going until we're stopped
    spark.streams.awaitAnyTermination
    // query.awaitTermination()

    spark.stop()

  }
}