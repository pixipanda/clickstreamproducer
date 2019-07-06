package  com.pixipanda.loggenerator

import java.util.{Calendar, Date}


import com.pixipanda.producer.{Producer, AvroClickStreamProducer}
import com.typesafe.config.Config
import org.apache.log4j.Logger

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.Source
import scala.util.Random
import scala.util.control.Breaks._



case class ApacheAccessLog(ipAddress: String, clientId: String,
                           userId: String, dateTime: String, method: String,
                           requestURI: String, protocol: String,
                           responseCode: Int, contentSize: Long)



case class ApacheAccessLogCombined(ipAddress: String, userId: String,
                                   clientId: String, dateTime: String,
                                   requestURI: String, responseCode: Int, contentSize: Long,
                                   referrer:String, useragent:String)



class EcommerceLogGenerator(config: Config) {


  val requestsFile = config.getString("requests")
  val referrersFile = config.getString("referrers")
  val ipaddressFile = config.getString("ipaddresses")
  val userAgentsFile = config.getString("user.agents")
  val evenCount = config.getString("event.count").toInt
  val maxFileLines = config.getString("max.file.lines").toInt
  val noOfCountries = config.getString("n.countries").toInt
  val maxIpsPerCountry = config.getString("max.ips.per.country").toInt
  val maxClicksPerUser = config.getString("max.clicks.per.user").toInt

  val ipA_by_ctry = Array.ofDim[Int](maxIpsPerCountry,noOfCountries)
  val ipB_by_ctry = Array.ofDim[Int](maxIpsPerCountry,noOfCountries)
  val tot_ips_by_ctry: Array[Int] = new Array[Int](noOfCountries)
  val cum_hourly_weight_by_ctry = Array.ofDim[Int](24,noOfCountries)
  val tot_weight_per_hour: Array[Int] = new Array[Int](24)
  var tot_weight_per_day = 0


  val ctry_ind: HashMap[String, String] = new HashMap[String, String]
  var n_requests: Int = 0
  var n_referrers: Int = 0
  var n_user_agents: Int = 0

  val requests: Array[String] = new Array[String](maxFileLines)
  val referrers: Array[String] = new Array[String](maxFileLines)
  val user_agents: Array[String] = new Array[String](maxFileLines)


  val logger = Logger.getLogger(getClass.getName)
  try {
    logger.info("Begin Intialization")
    var ctry_abbr: Array[String] = Array[String]("CH", "US", "IN", "JP", "BR", "DE", "RU", "ID", "GB", "FR", "NG", "MX", "KR", "IR", "TR", "IT", "PH", "VN", "ES", "PK")
    for (i <- 0 to noOfCountries - 1) {
      tot_ips_by_ctry(i) = 0
      ctry_ind.put(ctry_abbr(i), String.valueOf(i))
    }

    for (i <- 0 to maxIpsPerCountry - 1) {
      for (j <- 0 to noOfCountries - 1) {
        ipA_by_ctry(i)(j) = 0
        ipB_by_ctry(i)(j) = 0
      }
    }

    initCountryIP
    initRequests
    initReferrers
    initUserAgents
    weight




  }
  catch {
    case e: Exception => throw new RuntimeException(s"Initialization failed!: $e")
  }


  def initCountryIP: Unit = {
    for (line <- Source.fromFile(ipaddressFile).getLines()) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        val i_ctry_s: String = ctry_ind.getOrElse(fields(1), null)
        if (i_ctry_s != null) {
          val i_ctry = i_ctry_s.toInt
          val octets: Array[String] = fields(0).split("\\.")
          if (tot_ips_by_ctry(i_ctry) < maxIpsPerCountry) {
            ipA_by_ctry(tot_ips_by_ctry(i_ctry))(i_ctry) = octets(0).toInt
            ipB_by_ctry(tot_ips_by_ctry(i_ctry))(i_ctry) = octets(1).toInt
            tot_ips_by_ctry(i_ctry) += 1
          }
        }
      }
    }
  }


  def initRequests: Unit = {

    logger.info("Begin initRequest")
    var i = 0
    for (line <- Source.fromFile(requestsFile).getLines()) {
      requests(i) = line
      i += 1
    }
    n_requests = i

    logger.debug("Lines read= " + n_requests);
    for (i <- 0 to n_requests -1)
      logger.info("Requests line " + i + ": " + requests(i));

  }


  def initReferrers: Unit ={
    var i = 0
    for (line <- Source.fromFile(referrersFile).getLines()) {
      logger.debug(line)
      referrers(i) = line
      i += 1
    }
    n_referrers = i

    logger.debug("Lines read= " + n_requests);
    for (i <- 0 to n_requests -1)
      logger.debug("Referrers line " + i + ": " + requests(i));

  }

  def initUserAgents: Unit ={
    var i = 0
    for (line <- Source.fromFile(userAgentsFile).getLines()) {
      user_agents(i) = line
      i += 1
    }
    n_user_agents = i

    logger.debug("Lines read= " + n_requests);
    for (i <- 0 to n_requests -1)
      logger.debug("User agents line " + i + ": " + requests(i));

  }

  def weight: Unit= {

    var ctry_pct: Array[Int] = Array[Int](31, 13, 7, 5, 5, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 1, 1, 1)
    var hourly_weight: Array[Int] = Array[Int](4, 3, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 2, 2, 3, 4, 6, 8, 12, 12, 12, 10)
    var ctry_time_diff: Array[Int] = Array[Int](13, 0, 11, 14, 2, 7, 9, 12, 6, 7, 6, 0, 14, 10, 8, 7, 13, 12, 7, 10)
    var hourly_weight_by_ctry = Array.ofDim[Int](24,noOfCountries)


    for( hour <- 0 to 23){
      for(ctry <- 0 to noOfCountries - 1){
        val local_hour = (hour + ctry_time_diff(ctry)) % 24
        hourly_weight_by_ctry(hour)(ctry) = hourly_weight(local_hour) * ctry_pct(ctry)
      }
    }

    for( hour <- 0 to 23){
      var sum = 0
      for(ctry <- 0 to noOfCountries - 1){
        sum += hourly_weight_by_ctry(hour)(ctry)
        cum_hourly_weight_by_ctry(hour)(ctry) = sum
      }
      tot_weight_per_hour(hour) = sum
      tot_weight_per_day += sum;
    }
  }



  def generateEvent(producer: Producer) = {

    val c: Calendar = Calendar.getInstance
    c.setTime(new Date)
    c.add(Calendar.DATE, 1)
    val d = c.getTime


    val curr: Date = new Date
    var n_clicks_per_hour: Array[Int] = new Array[Int](24)
    var avg_time_between_clicks: Array[Double] = new Array[Double](24)
    var status: Array[Int] = Array[Int](200, 200, 200, 200, 200, 200, 200, 400, 404, 500)

    for(hour <- 0 to 23) {
      n_clicks_per_hour(hour) = Math.max(1, Math.floor(0.5 + evenCount.toDouble *
        (tot_weight_per_hour(hour).toDouble)/tot_weight_per_day.toDouble).toInt)
      avg_time_between_clicks(hour) = (3600.toDouble / n_clicks_per_hour(hour)) * 1000
      logger.info(" clicks: " + n_clicks_per_hour(hour) + " clickstime: " + avg_time_between_clicks(hour))
    }

    val rand: Random = new Random(curr.getTime)

    val time_of_day_in_sec: Double = 0.0
    val hour: Int = 0
    var clicks_left: Int = 0
    //var ip4: String = ""
    var referrer: String = ""
    var user_agent: String = ""

    var month_abbr: Array[String] = Array[String]("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

    val buffer = ArrayBuffer[String]()

    while(true) {
      val currdate: Date = new Date
      if (currdate.getTime > d.getTime) break //todo: break is not supported
      val h: Int = currdate.getHours

      val delay: Long = avg_time_between_clicks(h).toLong

      Thread.sleep(delay)
      val day: Int = currdate.getDate
      val month: Int = currdate.getMonth
      val year: Int = currdate.getYear + 1900

      // Pick random number for given hour, then look up country in cum weights, then pick random row for IP
      val r = 1 + rand.nextInt(tot_weight_per_hour(hour))

      var ctry = 0
      while (r > cum_hourly_weight_by_ctry(hour)(ctry)) {
        ctry += 1; ctry
      }


      val i = rand.nextInt(tot_ips_by_ctry(ctry))


      var ipv4: String = "%d.%d.%d.%d".format(ipA_by_ctry(i)(ctry), ipB_by_ctry(i)(ctry), 2 + rand.nextInt(249), 2 + rand.nextInt(249))

      clicks_left = 1 + rand.nextInt(maxClicksPerUser)
      referrer = referrers(rand.nextInt(n_referrers))
      user_agent = user_agents(rand.nextInt(n_user_agents))

      val timestamp: String =  "%02d:%02d:%02d".format(currdate.getHours, currdate.getMinutes, currdate.getSeconds)
      val output: String = "%s - %d [%02d/%3s/%4d:%8s -0500] \"%s\" %d %d \"%s\" \"%s\"\n".format( ipv4, i, day, month_abbr(month), year, timestamp, requests(rand.nextInt(n_requests)), status(rand.nextInt(10)), rand.nextInt(4096), referrer, user_agent)

      val datetime = f"[$day%02d/${month_abbr(month)}%3s/$year%4d:$timestamp%8s -0500]"

      val apacheLogEvent = ApacheAccessLogCombined(ipv4, "-", i.toString, datetime, requests(rand.nextInt(n_requests)), status(rand.nextInt(10)), rand.nextInt(4096).toLong, referrer, user_agent)
      logger.info("Output:" + output)

      producer.publish(apacheLogEvent)

    }


  }

}

