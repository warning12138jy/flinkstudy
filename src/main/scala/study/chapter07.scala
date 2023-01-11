package study

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object chapter07 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.addSource(new ClickSource)
      // 数据源中的时间戳是单调递增的，所以使用下面的方法，只需要抽取时间戳就好了
      // 等同于最大延迟时间是 0 毫秒
      .assignAscendingTimestamps(_.timestamp)
      .map(r => (r.user,1L))
      // 使用用户名对数据流进行分组
      .keyBy(_._1)
      // 设置 5 秒钟的滚动事件时间窗口
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      // 保留第一个字段，针对第二个字段进行聚合
      .reduce((r1,r2) => (r1._1,r1._2+r2._2))
      .print()
    env.execute("ReduceFunction")
  }

}
