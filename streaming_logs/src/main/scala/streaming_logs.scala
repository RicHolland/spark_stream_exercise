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

  case class Event(script: String, start_end: String, time: Long, flag: Int = 0){
    override def toString() = "{" + this.script + "," + this.start_end + "," + this.time.toString + "}"
  }

  def main(args: Array[String]) {
    args foreach println

    val conf = new SparkConf().setMaster("local[*]").setAppName("Stream Test")
    val ssc = new StreamingContext(conf, Seconds(20))

    ssc.checkpoint("spark/checkpoint/StreamingLogs")

    val brokers = Map("metadata.broker.list" -> "localhost:9092,localhost:9093,localhost:9094")
    val input = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc,
        brokers,
        Set(args(0))
      )

    val mapRDD = input.filter(tup => JSON.parseFull(tup._2) match {
        case None => false
        case _ => true
      }).flatMap(tup => M.unapply(JSON.parseFull(tup._2).get)).cache

    val start = parse(mapRDD filter (map => map contains "start"), "start")
    val end = parse(mapRDD filter (map => map contains "end"), "end")

    val unMatched = for {
      (id, (optStart, optEnd)) <- start fullOuterJoin end
      if ! (optStart.isDefined) | ! (optEnd.isDefined)
    } yield {
      if (optStart.isDefined) (id, optStart.get)
      else (id, optEnd.get)
    }

    val state = unMatched updateStateByKey(updateState _)
    //state.saveAsTextFiles("/user/h7743735/spark/log_stream/test6/debug/state/")

    val diffs = for {
      (id, (s, e)) <- (state union start) join end
    } yield {
      (id, e.time - s.time)
    }

    diffs.saveAsTextFiles("/user/h7743735/spark/log_stream/test6/results/")

    ssc.start()
    ssc.awaitTermination()

  }

  def parse(
      rdd: DStream[Map[String, Any]],
      start_end: String
    ): DStream[(Int, Event)] = {
    for {
      event <- rdd
      D(id) = event("id")
      S(script) = event("script")
      D(time) = event(start_end)
    } yield {
      (id.toInt, Event(script, start_end, time.toLong))
    }
  }

  // Update state by key function 
  def updateState(in: Seq[Event], existing: Option[Event]) = in match {
    // If no input this batch, check if event is over.
    case Nil => if (isOver(existing)) None else existing
    // Seq with one element.
    // If `start` add to state, if `end` reset flag for deletion next batch.
    case x +: Nil => if (x.start_end == "start") Option(x)
      else existing.flatMap(e => Some(eventOver(e)))
    // Seq with multiple elements. Should not happen.
    case x +: xs +: Nil => throw new Exception("Why has this happened!!!")
  }

  // TODO: Move to case class?
  // Checks if flag indicates a finished event
  // (i.e. one that can be removed from the state)
  def isOver(opt: Option[Event]): Boolean = {
    (opt flatMap{e: Event => Some(e match {
        case Event(_, _, _, 1) => true
        case _ => false
      })
    }).getOrElse(false)
  }

  // Switch flag to 1 to indicate a finished event
  def eventOver(event: Event) = event match {
    case Event(_, _, _, 1) => println("WARNING: Event " + event + " already over."); event
    case Event(x, y, z, _) => Event(x, y, z, 1)
  }
}
