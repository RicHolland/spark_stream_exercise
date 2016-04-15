import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka.KafkaUtils
import _root_.kafka.serializer.StringDecoder

object Test {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Stream Test")
        val ssc = new StreamingContext(conf, Seconds(60))

        ssc.checkpoint("spark/checkpoint/StateTest")

        val input = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, Map("metadata.broker.list" -> "localhost:9092,localhost:9093,localhost:9094"), Set(args(0)))

        val dStream = for {
            (kmk, v) <- input
        } yield {
            v.split(",") match {
                case Array(id, time) => (id.toInt, time)
                case _ => (-1, "")
            }
        }

        val state = dStream.updateStateByKey[String](updateState _)

        state.saveAsTextFiles("/user/h7743735/spark/updateStateTest/ust_")

        ssc.start()
        ssc.awaitTermination()

    }


    def updateState(vals: Seq[String], res: Option[String]): Option[String] = {
        if (vals.size != 0) {
            Option(res.getOrElse("").trim.split(" ") match {
                case Array("") => vals.last
                case Array(str) => str + " " + vals.last
                case Array(_, str) => str + " " + vals.last
            })
        } else {
            res
        }
    }

}