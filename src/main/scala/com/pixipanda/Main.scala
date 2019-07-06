package  com.pixipanda

import com.pixipanda.loggenerator.EcommerceLogGenerator
import com.pixipanda.producer.AvroClickStreamProducer


object Main {

  def main(args: Array[String]) {

    val configParser = new ConfigParser()
    val logGeneratorConfig = configParser.loadLogGeneratorConfig
    val ecommerceLogGenerator = new EcommerceLogGenerator(logGeneratorConfig)
    val kafkaConfig = configParser.loadKafkaConfig
    val kafkaProducer = new AvroClickStreamProducer(kafkaConfig)
    ecommerceLogGenerator.generateEvent(kafkaProducer)

  }

}