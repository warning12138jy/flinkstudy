package study

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

object chapter09 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
    // 为所有数据都指定同一个 key，可以将所有数据都发送到同一个分区
      .keyBy(_ => "key")
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .process(new UvCountByWindow)
      .print()
    env.execute("ProcessWindowFunction")
  }

}
class UvCountByWindow extends ProcessWindowFunction[Event,String,String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[Event], out: Collector[String]): Unit = {
    // 初始化一个 Set 数据结构，用来对用户名进行去重
    var userSet = Set[String]()
    //将所有用户名去重
    elements.foreach(userSet += _.user)
    // 结合窗口信息，包装输出内容
    val windowStart = context.window.getStart
    val windowEnd = context.window.getEnd
    out.collect(" 窗 口 ： " + new Timestamp(windowStart) + "~" + new
        Timestamp(windowEnd) + "的独立访客数量是：" + userSet.size)
  }
}