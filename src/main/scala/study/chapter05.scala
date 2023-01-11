package study

import com.mysql.cj.jdbc.JdbcStatement
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.sql.PreparedStatement
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit


object chapter05 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment =StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    //输出到本地文件
    val stream = env.addSource(new ClickSource)
    val fileSink = StreamingFileSink.forRowFormat(new Path("./output"),new SimpleStringEncoder[String]("utf-8"))

    //通过.withRollingPolicy()方法指定“滚动策略”
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
//    设置了在以下 3 种情况下，我们就会滚动分区文件：
//    ⚫ 至少包含 15 分钟的数据
//    ⚫ 最近 5 分钟没有收到新的数据
//    ⚫ 文件大小已达到 1 GB
          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
          .withMaxPartSize(1024*1024*1024)
          .build()
      )
      .build
    stream.map(_.toString).addSink(fileSink)
    //输出到kafka
    val properties:Properties = new Properties()
    properties.put("bootstrap.servers","hadoop01:9092")
    val stream1 = env.readTextFile("./output/clink.csv")
    stream1.addSink(new FlinkKafkaProducer[String](
      "clinks",new SimpleStringSchema(),properties
    )).setParallelism(1)

    //输出到Redis
    val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop01").build()
    env.addSource(new ClickSource).addSink(new RedisSink[Event](conf,new MyRedisMapper()))

    //输出到ES
    val stream2 = env.addSource(new ClickSource)
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop02",9200,"http"))
    val esBuilder = new ElasticsearchSink.Builder[Event](httpHosts,new ElasticsearchSinkFunction[Event] {
      override def process(t: Event, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val data = new util.HashMap[String,String]()
        data.put(t.user,t.url)
        val IndexRequest = Requests
          .indexRequest()
          .index("clinks")
          .`type`("type")
          .source(data)
        requestIndexer.add(IndexRequest)
      }
    }
    )
    stream2.addSink(esBuilder.build())
    //输出到Mysql
    val stream3 = env.addSource(new ClickSource)
    stream3.addSink(
      JdbcSink.sink(
        "INSERT INTO clinks(user,url) VALUES(?,?)",new JdbcStatementBuilder[Event] {
          override def accept(t: PreparedStatement, u: Event): Unit = {
            t.setString(1,u.user)
            t.setString(2,u.url)
          }
        },
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl("jdbc:mysql://localhost:3306/test")
          .withDriverName("com.mysql.jdbc.Driver")
          .withUsername("username")
          .withPassword("password")
          .build()
      )
    )
    env.execute("testSink")
  }

}

class MyRedisMapper extends RedisMapper[Event]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"clinks")
  }

  override def getKeyFromData(t: Event): String = {
    t.user
  }

  override def getValueFromData(t: Event): String = {
    t.url
  }
}
