package study

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.mutable.ListBuffer


// TODO:TOP-N
object chapter13 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val EventStream = env
      .addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
    // 只需要 url 就可以统计数量，所以抽取 url 转换成 String，直接开窗统计
    EventStream.map(_.url)
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      .process(new ProcessAllWindowFunction[String,String,TimeWindow] {
        override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
          // 初始化一个 Map，key 为 url，value 为 url 的 pv 数据
          val urlCountMap = Map[String,Long]()
          // 将 url 和 pv 数据写入 Map 中
          elements.foreach(
            r => urlCountMap.get(r) match {
              case Some(count) => urlCountMap.put(r,count+1L)
              case None => urlCountMap.put(r,1L)
            }

          )
          // 将 Map 中的 KV 键值对转换成列表数据结构
          // 列表中的元素是(K,V)元组
          var mapList = new ListBuffer[(String,Long)]()
          urlCountMap.keys.foreach(
            k => urlCountMap.get(k) match {
              case Some(count) => mapList += ((k,count))
              case None => mapList
            }
          )
          // 按照浏览量数据进行降序排列
          mapList.sortBy(_._2).reverse
          // 拼接字符串并输出
          val result = new StringBuilder
          result.append("===============================\n")
          for (i <- 0 to 1){
            val temp = mapList(i)
            result
              .append("浏览量 No." + (i + 1) + " ")
              .append("url: " + temp._1 + " ")
              .append("浏览量是：" + temp._2 + " ")
              .append("窗口结束时间是：" + new Timestamp(context.window.getEnd) + "\n")
          }
          result.append("===================================\n")
          out.collect(result.toString())
        }
      })
      .print()
    env.execute("TOP-N")

  }
}
