package study
import org.apache.flink.streaming.api.functions.co._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
object chapter16 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //TODO: 在电商网站中，某些用户行为往往会有短时间内的强关联。我们这里举一个例子，我们有
    // 两条流，一条是下订单的流，一条是浏览数据的流。我们可以针对同一个用户，来做这样一个
    // 联结。也就是使用一个用户的下订单的事件和这个用户的最近十分钟的浏览数据进行一个联结查询。
    // 订单事件流
    val orderStream = env.fromElements(
      ("Mary", "order-1", 5000L),
      ("Alice", "order-2", 5000L),
      ("Bob", "order-3", 20000L),
      ("Alice", "order-4", 20000L),
      ("Cary", "order-5", 51000L)
    ).assignAscendingTimestamps(_._3)
    // 点击事件流
    val pvStream = env.fromElements(
      Event("Bob", "./cart", 2000L),
      Event("Alice", "./prod?id=100", 3000L),
      Event("Alice", "./prod?id=200", 3500L),
      Event("Bob", "./prod?id=2", 2500L),
      Event("Alice", "./prod?id=300", 36000L),
      Event("Bob", "./home", 30000L),
      Event("Bob", "./prod?id=1", 23000L),
      Event("Bob", "./prod?id=3", 33000L)
    ).assignAscendingTimestamps(_.timestamp)

    // 两条流进行间隔联结，输出匹配结果
    orderStream
      .keyBy(_._1)
      .intervalJoin(pvStream.keyBy(_.user))
    //指定间隔
      .between(Time.minutes(-5),Time.minutes(10))
      .process(new ProcessJoinFunction[(String,String,Long),Event,String] {
        override def processElement(left: (String, String, Long), right: Event, ctx: ProcessJoinFunction[(String, String, Long), Event, String]#Context, out: Collector[String]): Unit = {
          out.collect(left+"=>"+right)
        }
      })
      .print()
    env.execute("IntervalJoinExample")
  }

}
