package study

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object chapter15 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // TODO:  在电商网站中，往往需要统计用户不同行为之间的转化，这就需要对不同的行为数据流，
    //  按照用户 ID 进行分组后再合并，以分析它们之间的关联。如果这些是以固定时间周期（比如1小时）来统计的，那我们就可以使用窗口 join 来实现这样的需求。
    val stream1 = env.fromElements(
      ("a", 1000L),
      ("b", 1000L),
      ("a", 2000L),
      ("b", 2000L)
    ).assignAscendingTimestamps(_._2)

    val stream2 = env.fromElements(
      ("a", 3000L),
      ("b", 3000L),
      ("a", 4000L),
      ("b", 4000L)
    )
      .assignAscendingTimestamps(_._2)
    stream1
      .join(stream2)
      .where(_._1) //指定第一条流中元素的key
      .equalTo(_._1) //指定第二条流中元素的key
    //开窗口
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new JoinFunction[(String,Long),(String,Long),String] {
        override def join(first: (String, Long), second: (String, Long)): String = {
          first + "=>" + second
        }
      })
      .print()
    env.execute("WindowJoinExample")


  }
}
