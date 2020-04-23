package sparkstreaming

import commons.conf.ConfigurationManager
import commons.constant.Constants
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object sparkstreaming {
  def main(args: Array[String]): Unit = {
    ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val conf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val streamingContext = new StreamingContext(sparkSession.sparkContext,Seconds(5))
    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    var  kafkaParam =Map(
      "boostrap.servers" -> kafka_brokers,
      "key.deserailizer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // auto.offset.reset
      // latest: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费；
      // earlist: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据开始消费
      // none: 先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)

    )
    val adRealTimeDStream = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )



  }
}
