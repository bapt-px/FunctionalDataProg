
import java.io._
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import scala.collection.JavaConverters._


object Consumer extends App {


  val TOPIC="dronesinfos"

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "myconsumergroup")

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(util.Collections.singletonList(TOPIC))

  getRegularDronesInfos(3000)

    def getRegularDronesInfos(period: Int): Unit =  {
    val t = new java.util.Timer()
    val task = new java.util.TimerTask {
      def run() = {
        var i : Int = 0
        println("fetching new drones data")
         val records=consumer.poll(3)
          for (record<-records.asScala){



            val fw = new FileWriter("drone_"+i+".JSON", true)
            try {
              fw.write(record.value()+"\n")
            }
            finally fw.close()

            i += 1
            println(record)
          }
      }
    }
    t.schedule(task, period, period)
    Analyze.compare()
    //task.cancel()
  }


}




