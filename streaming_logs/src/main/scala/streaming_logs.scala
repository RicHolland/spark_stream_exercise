import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import scala.util.parsing.json._
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream._

object StreamingLogs {

  /*Defines a base class with an implementation of the unapply method that
  casts object to start_end T*/
  class CC[T] { def unapply(a: Any): Option[T] = Some(a.asInstanceOf[T]) }

  //Singleton implementations of CC class
  object M extends CC[Map[String, Any]]
  object S extends CC[String]
  object D extends CC[Double]
  object B extends CC[Boolean]

  case class Event(script: String, start_end: String, time: Long){
    override def toString() = this.script + "," + this.start_end + "," + this.time.toString
  }

  def main(args: Array[String]) {
    args foreach println

    val conf = new SparkConf().setMaster("local[*]").setAppName("Stream Test")
    val ssc = new StreamingContext(conf, Seconds(60))

    ssc.checkpoint("spark/checkpoint/StreamingLogs")

    val input = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, Map("metadata.broker.list" -> "localhost:9092"), Set(args(0)))

    val mapRDD = input.filter(tup => JSON.parseFull(tup._2) match{
        case None => false
        case _ => true
      }).flatMap(tup => M.unapply(JSON.parseFull(tup._2).get)).cache

    val start = parse(mapRDD filter (map => map contains "start"), "start")
    val end = parse(mapRDD filter (map => map contains "end"), "end")

    val diffs = for {
      (id, (s, e)) <- start join end
    } yield {
      (id, e.time - s.time)
    }

    start.saveAsTextFiles("/user/h7743735/spark/log_stream/test2/end")
    end.saveAsTextFiles("/user/h7743735/spark/log_stream/test2/start")

    //val logTuples = input.flatMap(tup => logParse(tup._2))

    //logTuples.map(t => t._1 + "," + t._2 + "," + t._3.toInt).saveAsTextFiles("/user/h7743735/spark/log_stream/test1/json-logs", "csv")

    ssc.start()
    ssc.awaitTermination()

  }

  def parse(rdd: DStream[Map[String, Any]], start_end: String): DStream[(Int, Event)] = {
    for {
      event <- rdd
      D(id) = event("id")
      S(script) = event("script")
      D(time) = event(start_end)
    } yield {
      (id.toInt, Event(script, start_end, time.toLong))
    }
  }

  /**
  Function takes string, parses as JSON object
  **/
  def logParse(str: String): List[(String, Boolean, Double)] = {

    
    for {
    /*Parse JSON string - instatiate list using result
      If JSON Array is returned, list of list returned #But this shouldn't happen#
      If JSON Object, list of one element (check this)
      Cast all members of list to Map[String, Any]
    */
    Some(M(map)) <- List(JSON.parseFull(str))
    //extract value for key "jobName", cast to String
    S(jobName) = map("jobName")
    //extract value for key "success", cast to Boolean
    B(success) = map("success")
    //extract value for key "exit", cast to Double
    D(exitCode) = map("exit")
  }
  yield {
    (jobName, success, exitCode)
    //Could use own class with relevant fields
  }

}
