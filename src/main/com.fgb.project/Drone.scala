import scala.io.Source
import play.api.libs.json._
import play.api.libs.json.Reads._


package drone
{

  class Drone(val id: Int) {


    var id_drone: Int = id

    // battery in percentage
    var battery: Int  = 100

    // altitude in meters
    var altitude: Int  = 0

    // temperature in degrees Celsius
    var temperature: Int  = 0

    // Speed in km/h
    var speed: Int  = 0

    // Remaining disk space in MB
    var disk_space: Int  = 1000

    // time
    var time: Int  = 0

    // Default latitude and longitude is Paris
    var lat: Double  = 48.8534
    var long: Double  = 2.3488

    def minimumInfos() {
      println ("drone id: " + id_drone);
      println ("Point x lat : " + lat);
      println ("Point y long : " + long);
    }


    def getJsonString(): String = {
      // Creating JSON
      val drone_json: JsValue = Json.parse(s"""
      {
        "id_drone" : ${id_drone},
        "battery" : ${battery},
        "altitude" : ${altitude},
        "time" : ${time},
        "temperature" : ${temperature},
        "speed": ${speed},
        "disk_space": ${disk_space},
        "location" : {
          "lat" : ${lat},
          "long" : ${long}
        }
      }
      """)

      Json.stringify(drone_json)
    }

    def changeInfos() {
      val r = scala.util.Random
      temperature = 30 + 1 // simulation of temperature increase
      battery = battery - 1 // simulation of battery decrease
      altitude = altitude + 1 + r.nextInt(3) // simulation of altitude increase
      speed = 10 + r.nextInt(50) // speed oscilliate between 30 and 50
      disk_space = disk_space - r.nextInt(20 + r.nextInt(10)) // drone loose bewteen 20 and 30 MB of disk space because movie filming
      lat = lat + r.nextDouble
      long = long + r.nextDouble
      time += 30
    }

  }


}


