import java.text.SimpleDateFormat

val timestampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
val startDate = "2018-04-10T13:30:34.45Z";
val  start = new SimpleDateFormat(timestampFormat).parse(startDate);

println("Data convertita: "+start.toString)



def maxTimeStamp(t1: String, t2: String): String = {

  val t1d = new SimpleDateFormat(timestampFormat).parse(t1)
  val t2d = new SimpleDateFormat(timestampFormat).parse(t2)
  if (t1d.after(t2d)) t1 else t2

}



println(maxTimeStamp("2020-04-10T13:30:34.45Z","2018-02-10T13:30:34.45Z"))