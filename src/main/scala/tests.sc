import org.apache.spark.sql.Column

import scala.annotation.tailrec

val a = Array("pippo","pluto","minnie").toList

import org.apache.spark.sql.functions._

def dup(a: List[String]): List[Column] = {
  def dupAccum(aa: List[String], acc: List[Column]): List[Column] = {
    aa match  {
      case Nil => acc
      case x :: tail => {
                          val n = acc ::: List(lit(x+"=")) ::: List(col(x))
                          dupAccum(tail, n)
                        }
    }
  }
  dupAccum(a,Nil)
}

dup(a)



def sum2(ints: List[Int]): Int = {
  @tailrec
  def sumAccumulator(ints: List[Int], accum: Int): Int = {
    ints match {
      case Nil => accum
      case x :: tail => sumAccumulator(tail, accum + x)
    }
  }
  sumAccumulator(ints, 0)
}