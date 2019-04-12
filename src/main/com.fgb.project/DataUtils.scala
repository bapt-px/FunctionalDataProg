import com.google.gson.Gson

object DataUtils {

  case class Location (
                        lat: Float,
                        long: Float
                      )

  case class Data (
                    id_drone: Int,
                    time: Int,
                    battery: Int,
                    altitude: Float,
                    temperature: Float,
                    speed: Float,
                    disk_space: Int,
                    location: Location
                  )

  def parseFromJson(lines:Iterator[String]):Iterator[Data] = {
    val gson = new Gson
    lines.map(line => {
      gson.fromJson(line, classOf[Data])
    })
  }

}


object TweetUtils {

  case class Tweet(
                    id: String,
                    user: String,
                    text: String,
                    place: String,
                    country: String,
                    lang: String
                  )

  def parseFromJson(lines: Iterator[String]): Iterator[Tweet] = {
    val gson = new Gson
    lines.map(line => gson.fromJson(line, classOf[Tweet]))
  }
}