import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.text.SimpleDateFormat

case class MyRow1(key: String, timeStamp: String, as_of_date: String)


object MaxAggregatorTest2 {

  val timestampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

  def maxTimeStamp(t1: String, t2: String): String = {
    val t1d = new SimpleDateFormat(timestampFormat).parse(t1)
    val t2d = new SimpleDateFormat(timestampFormat).parse(t2)
    if (t1d.after(t2d)) t1 else t2
  }

  val customAggregator: TypedColumn[MyRow1, String] = new Aggregator[MyRow1, String, String]
  {
    def zero: String = "1000-01-01T00:00:00.00Z"
    def reduce(acc: String, x: MyRow1): String = maxTimeStamp(acc,x.timeStamp)
    def merge(acc1: String, acc2: String): String = maxTimeStamp(acc1,acc2)
    def finish(acc: String): String = acc
    def bufferEncoder: Encoder[String] = Encoders.STRING
    def outputEncoder: Encoder[String] = Encoders.STRING

  }.toColumn


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    import sparkSession.implicits._

    val df = Seq(( "20200131,1", "2018-01-10T13:30:34.45Z","20200101"),
      ("20200131,1", "2018-02-10T13:30:34.45Z","20200101"),
      ("20200131,1", "2018-03-10T13:30:34.45Z","20200101"),
      ("20200131,2", "2018-01-10T13:30:34.45Z","20200101"),
      ("20200228,1", "2018-11-10T13:30:34.45Z","20200101"))
      .toDF("key", "timestamp","as_of_date").as[MyRow1]

    df.show(false)
    val res = df.groupByKey(k => (k.as_of_date,k.key)).agg(customAggregator.name("MaxTimestamp"))

    res.printSchema()

    val res1 = res.select("key.*","MaxTimestamp").withColumnRenamed("_1","as_of_date").withColumnRenamed("_2","key")

    res1.show(false)


  }

}
