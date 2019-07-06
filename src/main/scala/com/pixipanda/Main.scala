package  com.pixipanda

import com.pixipanda.clickstreamgenerator.ClickStreamGenerator
import com.pixipanda.producer.AvroClickStreamProducer


object Main {

  def main(args: Array[String]) {

    val configParser = new ConfigParser()
    val logGeneratorConfig = configParser.loadLogGeneratorConfig
    val clickstreamGenerator = new ClickStreamGenerator(logGeneratorConfig)
    val kafkaConfig = configParser.loadKafkaConfig
    val kafkaProducer = new AvroClickStreamProducer(kafkaConfig)
    clickstreamGenerator.generateEvent(kafkaProducer)

  }

}