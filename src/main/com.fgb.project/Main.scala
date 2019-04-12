
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.io.File

object sparkProject {

    val pathToFile = "data.json"


    /**
     *  Load the data from the text file and return an RDD of words
     */
    def loadData(): RDD[DataUtils.Data] = {
        // create spark configuration and spark context: the Spark context is the entry point in Spark.
        // It represents the connexion to Spark and it is the place where you can configure the common properties
        // like the app name, the master url, memories allocation...
        val conf = new SparkConf()
            .setAppName("droneData")
            .setMaster("local[*]") // here local mode. And * means you will use as much as you have cores.

        val sc = SparkContext.getOrCreate(conf)


        // load data and create an RDD where each element will be a word
        // Here the flatMap method is used to separate the word in each line using the space separator
        // In this way it returns an RDD where each "element" is a word


        sc.textFile("data/drone_[0-999].json")
            .mapPartitions(DataUtils.parseFromJson(_))

    }

    def compareForTemperature() {
        val r = loadData()
          .map(e => ((e.temperature.toInt, e.id_drone), (e.altitude, e.location.lat, e.location.long, e.speed, 1)))
          .reduceByKey((acc, i) =>
              (i._1 + acc._1,
                acc._2 + i._2,
                acc._3 + i._3,
                acc._4 + i._4, acc._5 + i._5)
          )
          .map(tuple => (tuple._1._1,
            tuple._2._1 / tuple._2._5
            , tuple._2._2 / tuple._2._5
            , tuple._2._3 / tuple._2._5
            , tuple._2._4 / tuple._2._5
            , tuple._2._5 / tuple._2._5
            , tuple._1._2 )
          )
          .sortBy(_._1)

        r.foreach(e => println("Drone " + e._7  + " : " + e._1 + "°C : Pos(" + e._3 + ", " + e._4 + ") Alt : " + e._2 + "  speed : " + e._5 ))

    }

    def compareForBattery(): Unit = {
        val r = loadData()
          .map(e => ((e.battery.toInt, e.id_drone), (e.speed, 1)))
          .reduceByKey((acc, i) => (i._1 + acc._1, acc._2 + i._2)   )
          .map(tuple => (tuple._1._1,
            tuple._2._1 / tuple._2._2,
            tuple._2._2,
            tuple._1._2)
          )
          .sortBy(_._1)

        r.foreach(e => println("Drone " + e._3  + " : " + e._1 + "% : Moyenne : " + e._2 + "Km/h, pour " + (e._3 * 30) + " secondes" ))

    }

    def isDefaillant(): Unit = {
        val r = loadData()
          .foreach(e => {
              if(e.temperature > 50) println("high temperature detected on drone " + e.id_drone)
          })
    }

    def hasRecord(tuples: Array[(Int, Float)], i: Int): Unit = {
        var j = 0
        for( j <- i + 1 to tuples.length) {
            if (tuples(j)._1 == tuples(i)._1) return true
        }
        return false
    }


    def hasFall(): Unit = {

        val r = loadData().
          sortBy(_.time)
          .map(e => (e.id_drone, e.altitude))
          .collect()

        var i = 0
        for( i <- 0 to r.length - 2) {
            if (r(i + 1)._2 - r(i)._2 > 50 && hasRecord(r, i) == false) {
                println(r(i)._1 + " got trouble")
            }
        }
    }

    def compare(args: Array[String]): Unit = {


        compareForTemperature()

        compareForBattery()
        hasFall()

    }

}