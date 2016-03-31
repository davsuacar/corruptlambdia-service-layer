import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by davidsuarez on 22/03/16.
  */
object CorruptServiceLayer extends App {

  val logFileBatch = "/tmp/corrupt/batch/part-*"
  val logFileStreaming = "/tmp/corrupt/streaming/part-*"

  val conf = new SparkConf().setMaster("local[2]").setAppName("CorruptLayer")
  val sc = new SparkContext(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  case class Payment(name: String, amount: Int)

  val file1 = sc.textFile(logFileBatch)
  val file2 = sc.textFile(logFileStreaming)

  // Input Dataframe from batch
  val corruptBatchPaymnentRDD = file1.map({
    x => x.replace("(", "").replace(")","").trim().split(",")
  }).map(x => {
    Payment(x(0), x(1).trim.toInt)
  }).toDF()

  // Temporary Table created
  corruptBatchPaymnentRDD.registerTempTable("corruptBatchPaymnent")

  // Input Dataframe from streaming
  val corruptStreamingPaymnentRDD = file2.map({
    x => x.replace("(", "").replace(")","").trim().split(",")
  }).map(x => {
    Payment(x(0), x(1).trim.toInt)
  }).toDF()

  // Temporary Table created
  corruptStreamingPaymnentRDD.registerTempTable("corruptStreamingPaymnent")

  // Query to extract info from two tables
  val select = sqlContext.sql("SELECT tb1.name, tb1.amount, tb2.amount FROM corruptBatchPaymnent as tb1 LEFT JOIN corruptStreamingPaymnent as tb2 ON tb1.name = tb2.name")

  // Show payments from batch and streaming separated
  select.foreach(println)

  // Show payments from batch and streaming added
  select.map(x => (x.getString(0), x.getInt(1) + x.getInt(2))).foreach(println)
}
