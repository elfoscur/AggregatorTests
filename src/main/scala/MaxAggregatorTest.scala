import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

case class MyRow(date: String, key: String, timeStamp: Int)


object MaxAggregatorTest {


  val timestampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

  def AggregateLatestTimestamp(in: Dataset[MyRow]): Dataset[MyRow] = {
    in
  }

  val customAggregator: TypedColumn[MyRow, Int] = new Aggregator[MyRow, Int, Int]
  {
    def zero: Int = -1
    def reduce(acc: Int, x: MyRow): Int = Math.max(acc,x.timeStamp)
    def merge(acc1: Int, acc2: Int): Int = Math.max(acc1,acc2)
    def finish(acc: Int): Int = acc
    def bufferEncoder: Encoder[Int] = Encoders.scalaInt
    def outputEncoder: Encoder[Int] = Encoders.scalaInt

  }.toColumn


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    import sparkSession.implicits._

    val df = Seq(("20200131", "20200131,1", 20200105),
      ("20200131", "20200131,1", 20200110),
      ("20200131", "20200131,1", 20200120),
      ("20200131", "20200131,2", 20200101),
      ("20200228", "20200228,1", 20200201))
      .toDF("date", "key", "timestamp").as[MyRow]

    df.show()
    df.groupByKey(_.key).agg(customAggregator).show()

  }

}
