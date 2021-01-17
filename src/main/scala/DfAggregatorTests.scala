

object DfAggregatorTests {
  /*  def sum(a: Int, b: Int): Int = {
      return a + b
    }


    def doSomething(f: (Int,Int,Int) => Int,a: Int, b: Int, c: Int): String =  {

      val tmp: Int = f(a,b,c)
      val str: String = "la funzione ritorna: " + tmp.toString()
      return str
    }


    /* doSomething ha 4 parametri, una funzione nella forma (Int,Int,Int) => Int e 3 interi
       la funzione puo' essere definita ad esempio come:

       (x, y, z) => x+y+z

       ma anche usando i placeholder _

       con _+_+_


    */
    println(doSomething(_+_+_, 1, 2, 3))

    println(doSomething( (x, y, z) => x +y+z, 1, 2, 3) )


    def SayHello(callback: () => Unit): Unit  = {
      callback()
  }


  def Hello(): Unit = println("Hello")

  SayHello(Hello)




    def main(args: Array[String]): Unit = {
      val items = Seq(("candy", 2, true), ("cola", 7, false), ("apple", 3, false), ("milk", 4, true))
      val itemsToBuy = items
        .filter(_._3)  // filter in only available items (true)
        .filter(_._2 > 3)  // filter in only items with price greater than 3
        .map(_._1)  // return only the first element of the tuple; the item name

      itemsToBuy.foreach(println(_))



      val a = List(1,2,3,4)
      print(a.reduce(sum))


    }
*/
}
