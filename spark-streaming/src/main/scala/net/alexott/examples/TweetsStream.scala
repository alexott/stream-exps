package net.alexott.examples

import java.text.SimpleDateFormat
import java.util.Locale

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.avro.generic.GenericRecord
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.retry.RetryUntilElapsed
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.OffsetRange
import org.elasticsearch.spark.sparkRDDFunctions
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.joda.time.format.ISODateTimeFormat

import com.fasterxml.jackson.databind.ObjectMapper

import io.confluent.kafka.serializers.KafkaAvroDecoder
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata

case class TwitterUser(id: Long, name: String, screenName: String)
case class TwitterStatus(id: Long, createdAt: String, favoriteCount: Int, text: String, 
    user: TwitterUser, retweet: Boolean, hashTags: Array[String], mentions: Array[String])

object TweetsStream {
  
  def main(args: Array[String]) {
    // constants/parameters - adjust code here...
    val topic = "tweets2" // topic name to read from
    val groupName = "tweetsTest" // group name - will be used as namespace in ZooKeeper
    val zooServers = "localhost:2181" // ZooKeeper servers
    val schemaRegistry = "http://localhost:8081" // Schema Registry server
    val kafkaBrokers = "localhost:9092" // Kafka brokers
    
    // 
    val conf = new SparkConf().setAppName("TweetsStream")
    conf.set("es.index.auto.create", "true")
    
    val ssc = new StreamingContext(conf, Seconds(30))
    
    val curatorFramework = CuratorFrameworkFactory.builder()
      .namespace(groupName)
      .connectString(zooServers)
      .connectionTimeoutMs(5000)
      .sessionTimeoutMs(20000)
      .retryPolicy(new RetryUntilElapsed(2000, 2000))
      .build()
    curatorFramework.start()
    while (curatorFramework.getState != CuratorFrameworkState.STARTED) {
      Thread.sleep(1000)
    }
    
    val objectMapper = new ObjectMapper()

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> kafkaBrokers,
      "schema.registry.url" -> schemaRegistry)
    
    // read offsets if they exists
    val offsets = scala.collection.mutable.HashMap.empty[TopicAndPartition, Long]
    val basePath = buildBasePath(groupName, topic)
    // TODO: offsets should be checked for validity...
    // see https://github.com/juanrh/spark/blob/master/external/kafka/src/main/scala/org/apache/spark/streaming/kafka/KafkaUtils.scala#L403
    if(curatorFramework.checkExists().forPath(basePath) != null) {
      val childrens = curatorFramework.getChildren().forPath(basePath)
      childrens.foreach(x => {
        val partitionId = x.toInt
        val nodePath = buildNodePath(groupName, topic, partitionId)
        if(curatorFramework.checkExists().forPath(nodePath) != null) {
          val d = curatorFramework.getData().forPath(nodePath)
          val offset = objectMapper.readValue(d, classOf[Long])
          offsets.put(TopicAndPartition(topic, partitionId), offset)
        }
      })
    }
 
    // setup stream 
    val tweetsRaw = if (offsets.isEmpty) {
    // if offsets aren't found, then it should use smallest...
      println("Starting with smallest offsets...")
      val topicSet = Set(topic)
      val newKafkaParams = kafkaParams + ("auto.offset.reset" -> "smallest")
      KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder](ssc, newKafkaParams, topicSet)
    } else {
      println(s"Starting offsets: ${offsets}")
      val messageHandler = (mmd: MessageAndMetadata[Object, Object]) => (mmd.key, mmd.message)
      KafkaUtils.createDirectStream[Object, Object, KafkaAvroDecoder, KafkaAvroDecoder,
        (Object, Object)](ssc, kafkaParams, offsets.toMap, messageHandler)
    }
    
    var offsetRanges = Array[OffsetRange]()
    tweetsRaw.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.map {
      case (k,v) => val stat = objToStatus(v); stat 
    }.foreachRDD(rdd => {
      rdd.saveToEs("tweets-{createdAt:YYYY.MM.dd}/tweets", Map("es.mapping.id" -> "id"))
      for (o <- offsetRanges) {
        val offsetBytes = objectMapper.writeValueAsBytes(o.untilOffset)
        val nodePath = buildNodePath(groupName, o.topic, o.partition)
        println(s"${nodePath} ${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        if(curatorFramework.checkExists().forPath(nodePath) != null) {
          curatorFramework.setData().forPath(nodePath, offsetBytes)
        } else {
          curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
      
  }

  def buildBasePath(groupName: String, topic: String): String = {
    "/consumers/" + groupName + "/offsets/" + topic
  }
  
  def buildNodePath(groupName: String, topic: String, partition: Int): String = {
    buildBasePath(groupName, topic) + "/" + partition
  }
  
  val toFormat = ISODateTimeFormat.dateTime()
  
  def formatTime(o: String): String = {
    val date = try {
      val fromFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.US)
      fromFormat.parse(o) 
    } catch {
      case e: Exception => new DateTime(DateTimeZone.UTC) 
    } 
    toFormat.print(new DateTime(date, DateTimeZone.UTC))
  }
  
  // TODO: add extraction of places, names, etc.
  
  val hashTagRE = "#(\\p{Alpha}[\\p{Alnum}_]*)".r
  val mentionsRE = "@([a-zA-Z0-9_]{1,15})".r
  def objToStatus(v: Object): TwitterStatus = {
      val record = v.asInstanceOf[GenericRecord]
      val text = record.get("text").toString()
      val id = record.get("id").asInstanceOf[Long]
      val favCount = record.get("favoriteCount").asInstanceOf[Int]
      val created = record.get("createdAt").toString()
      val userRec = record.get("user").asInstanceOf[GenericRecord]
      val uid = userRec.get("id").asInstanceOf[Long]
      val name = userRec.get("name").toString()
      val sname = userRec.get("screenName").toString()
      val retweet = text.contains("RT @")
      val hashTags = hashTagRE.findAllIn(text).map { x => x.substring(1) }.toArray
      val mentions = mentionsRE.findAllIn(text).map { x => x.substring(1) }.toArray
      
      new TwitterStatus(id, formatTime(created), favCount, text, 
          new TwitterUser(uid, name, sname), retweet, hashTags, mentions)
  }
  
}
