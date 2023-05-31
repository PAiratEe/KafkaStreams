import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}

import java.io.IOException
import java.sql.DriverManager
import java.util.Properties

object KafkaStreams {

  def main(args: Array[String]) {
    val config: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "application")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      p.put(StreamsConfig.STATE_DIR_CONFIG, "hdfs://localhost:9000/kafkaTemplate/")
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "at_least_once")
      p.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
      p
    }

    val builder: KStreamBuilder = new KStreamBuilder()
    val textLines: KStream[String, String] = builder.stream("wikimedia_recentchange2")
    val necessaryMessages: KStream[String, String] = textLines
      .filter((key, textLine) => textLine.contains("\"bot\":true"))

    val connect = DriverManager.getConnection("jdbc:postgresql://localhost:5432/kafka-streams", config)
    val statement = connect.createStatement()

    val time = System.nanoTime
    var count = 0

    necessaryMessages.foreach((key, unit) => {
      try {
        count += 1
        val sql = s"INSERT INTO data (column2, column3) VALUES('${unit.substring(0, unit.indexOf("\n"))}', '${(System.nanoTime() / 1e9d)}')"
        statement.executeUpdate(sql)
      }
      catch {
        case c: IOException =>
          println("Данная операция была прервана " + c.printStackTrace())
      }
    })

    val streams: KafkaStreams = new KafkaStreams(builder, config)
    streams.metrics()
    streams.start()

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      streams.close()
      statement.close()
      connect.close()
    }))

  }
}
