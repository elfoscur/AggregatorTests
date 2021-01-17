import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

case class Food(key: String, date: Int, numeric: Int, text: String)

object AggregateLatestFoods {
  import org.apache.spark.sql.expressions.Aggregator
  import org.apache.spark.sql.{Encoder, Encoders, Row}

  case class Max(col: String)
    extends Aggregator[Row, Int, Int] with Serializable {

    def zero = Int.MinValue
    def reduce(acc: Int, x: Row) =
      Math.max(acc, Option(x.getAs[Int](col)).getOrElse(zero))

    def merge(acc1: Int, acc2: Int) = Math.max(acc1, acc2)
    def finish(acc: Int) = acc

    def bufferEncoder: Encoder[Int] = Encoders.scalaInt
    def outputEncoder: Encoder[Int] = Encoders.scalaInt
  }

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    import sparkSession.implicits._


    val df = Seq((1, None, 3), (4, Some(5), -6)).toDF("x", "y", "z")

    @transient val exprs = df.columns.map(c => Max(c).toColumn.alias(s"max($c)"))

    val df1 = df.agg(exprs.head, exprs.tail: _*)

    df.show()
    df1.show()
  }
}