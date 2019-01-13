//Import all required headers

import java.util.Properties
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import net.liftweb.json._
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer
import org.apache.flink.util.Collector
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.ArrayList
import java.util.HashMap
import java.util.List
import java.util
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.functions.RuntimeContext
import java.time.{Instant, ZoneId, ZonedDateTime}

import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import java.time.format.DateTimeFormatter
import java.time.LocalDate

//Custom mapping for the data stream we are reading from International Bike Channel

case class BikeStream(lon: Option[String], lat: Option[String],longitude: Option[String],latitude: Option[String],name: Option[String],address: Option[String],location: Option[String],
num_bikes_available : Option[Int],is_installed : Option[String],is_returning : Option[String],last_reported : Option[Long],country_code: Option[String],channel : Option[String], publisher :Option[String],station_id : Option[String],lastCommunicationTime : Option[String],is_renting : Option[String] )

object Main extends App {

    println("Starting the Bike Data Streaming application.......")
    implicit val formats = DefaultFormats
    //Creating a context of StreamExecutionEnvironment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    // comma separated list of Kafka brokers
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // id of the consumer group
    properties.setProperty("group.id", "test")
    //Adding a data source which is a kafka topic here
    properties.setProperty("group.id", "test")
    //Adding a data source which is a kafka topic here
    val stream = env
      // bike-channel is our Kafka topic
      .addSource(new FlinkKafkaConsumer010[String]("bike-channel", new SimpleStringSchema(), properties))


    val config = new util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    //specifying the cluster name for elasticsearch sink
    config.put("cluster.name", "elasticsearch") //default cluster name: elasticsearch

    val transports = new util.ArrayList[InetSocketAddress]
    transports.add(new InetSocketAddress(InetAddress.getByName("172.31.90.14"), 9300))


    //defining the addSink function to send data to an elasticsearch sink
    stream.addSink(new ElasticsearchSink(config, transports, new ElasticsearchSinkFunction[String] {
          def createIndexRequest(element: String): IndexRequest = {
              val json = new util.HashMap[String, AnyRef]
              //Parsing the string element to json
              val jValue = parse(element)
              val bikeStream = jValue.extract[BikeStream]
              //formatting the location field, merging the latitude and longitude value and creatinf a tuple
              var i= 0
              var j=0
              val last = bikeStream.last_reported
              var temp1 = ""
              var temp2 = ""
              if(bikeStream.lon == None) {//if lon field is not present use longitude field
                   temp1 = bikeStream.latitude.get
                   temp2 = bikeStream.longitude.get
                     }
              else
                   {
                   temp1 = bikeStream.lat.get
                   temp2 = bikeStream.lon.get
                   }
              //select the substring length based on the sign of the latitude or longtitude
              if(temp1.charAt(0) == '-') { i= 6}
                     else { i = 5}
              if(temp2.charAt(0) == '-') { j= 6}
                     else { j = 5}

              val str1 = temp1.substring(0,i).concat(",")
              var location= str1.concat(temp2.substring(0,j))

              //adding location field
              json.put("location", location)
              //adding location field
              json.put("location", location)
              //pick up location or address field whichever is present
              if(bikeStream.address == None){
                        json.put("address", bikeStream.location.getOrElse("NotFound"))}
              else{
                        json.put("address", bikeStream.address.getOrElse("NotFound"))
                  }

              //adding other fields
              json.put("num_bikes_available", bikeStream.num_bikes_available)
              json.put("is_installed", bikeStream.is_installed)
              json.put("is_renting", bikeStream.is_renting)
              //There may be last_reported field or lastCommunicationTime field, picking up either based on how they are present in datastream
              if(bikeStream.last_reported == None){ //if last_reported filed not present use lastComm field
                    val date = bikeStream.lastCommunicationTime.get.substring(0,19)
                    json.put("last_reported", date)
                }
              else{
                  //formatting the time to YYYY:mm:dd hh:mm:ss
                   val instant = Instant.ofEpochMilli((last.get*1000))
                   val parser1 = instant.toString
                   val parser2 = parser1.substring(0,10)+" "+parser1.substring(11,19)

                   json.put("last_reported", parser2)
                   }
               //pick up the country code
               json.put("country_code", bikeStream.country_code.getOrElse("NotFound"))
               //pick up the name field
               json.put("name", bikeStream.name.getOrElse("NotFound"))
               //pick up the channel name
               json.put("channel", bikeStream.channel.getOrElse("NotFound"))
               //pick up the publisher name
               json.put("publisher", bikeStream.publisher.getOrElse("NotFound"))
               //pick up the station id
               json.put("station_id", bikeStream.station_id.getOrElse("NotFound"))

               Requests.indexRequest.index("bike-channel").`type`("bike").source(json)
      }
     //overriding process function
     override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) {
     indexer.add(createIndexRequest(element))
                            }
      }))

    env.execute("Bike Streaming")
  }

