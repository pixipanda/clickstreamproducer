package com.pixipanda.producer

import java.util.Properties

import com.pixipanda.avro.ClickStream
import com.pixipanda.loggenerator.ApacheAccessLogCombined
import com.typesafe.config.Config
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}


class  AvroClickStreamProducer(config:Config) extends Producer{

  val prop = createProducerConfig()
  val producer = new KafkaProducer[String, ClickStream](prop)
  val topic = config.getString("topic")

  def createProducerConfig() = {
    val props = new Properties()
    props.put("bootstrap.servers", config.getString("bootstrap.servers"))
    props.put("acks", config.getString("acks"))
    props.put("retries", config.getString("retries"))
    props.put("batch.size", config.getString("batch.size"))
    props.put("linger.ms", config.getString("linger.ms"))
    props.put("buffer.memory", config.getString("buffer.memory"))
    props.put("key.serializer", config.getString("key.serializer"))
    props.put("value.serializer", config.getString("value.serializer"))
    props.put("schema.registry.url", config.getString("schema.registry.url"))
    props.put("auto.register.schemas",config.getString("auto.register.schemas"))

    props
  }


  def publish(event:Object) = {

      publishPageViewEvent(event)
  }


  def publishPageViewEvent(event: Object) = {

    val apacheRecord = event.asInstanceOf[ApacheAccessLogCombined]
    try {

      val pageView = new ClickStream()
      pageView.setIpv4(apacheRecord.ipAddress)
      pageView.setClientId(apacheRecord.clientId)
      pageView.setTimestamp(apacheRecord.dateTime)
      pageView.setRequestUri(apacheRecord.requestURI)
      pageView.setStatus(apacheRecord.responseCode)
      pageView.setContentSize(apacheRecord.contentSize.toInt)
      pageView.setReferrer(apacheRecord.referrer)
      pageView.setUseragent(apacheRecord.useragent)
      val producerRecord = new ProducerRecord[String, ClickStream](topic, pageView)
      producer.send(producerRecord)

    } catch {
      case ex: Exception => {
        ex.printStackTrace(System.out)
      }
    } finally {
      //producer.close
    }
  }
}