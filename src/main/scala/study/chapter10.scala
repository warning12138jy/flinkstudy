package study

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// TODO: 里统计 10 秒钟的 url 浏览量，每 5 秒钟更新,结合增量聚合函数和全窗口函数来得到统计结果
object chapter10 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
    // 使用 url 作为 key 对数据进行分区
      .keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
    // 注意这里调用的是 aggregate 方法
    // 增量聚合函数和全窗口聚合函数结合使用
      .aggregate(new UrlViewCountAgg,new UrlViewCountResult)
      .print()


    env.execute("AggregateFunctionAndProcessWindowFunction")
  }

}
/*
AggregateFunction 可以看作是 ReduceFunction 的通用版本，这里有三种类型：输入类型
（IN）、累加器类型（ACC）和输出类型（OUT）。输入类型 IN 就是输入流中元素的数据类型；
累加器类型 ACC 则是我们进行聚合的中间状态类型；而输出类型当然就是最终计算结果的类
型了。
 */
class UrlViewCountAgg extends AggregateFunction[Event,Long,Long]{
  override def createAccumulator(): Long = 0L
  //每来一个事件就加1
  override def add(value: Event, accumulator: Long): Long = accumulator + 1L
  //窗口闭合时发送的计算结果
  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = ???
}

/*
这里我们为了方便处理，单独定义了一个样例类 UrlViewCount 来表示聚合输出结果的数
据类型，包含了 url、浏览量以及窗口的起始结束时间。用一个 AggregateFunction 来实现增量
聚合，每来一个数据就计数加一；得到的结果交给 ProcessWindowFunction，结合窗口信息包
装成我们想要的 UrlViewCount，最终输出统计结果。
 */
case class UrlViewCount(url: String, count: Long, windowStart: Long, windowEnd: Long)
/*
除了可以拿到窗口中的所有数据之外，ProcessWindowFunction 还可以获取到一个
“上下文对象”（Context）。这个上下文对象非常强大，不仅能够获取窗口信息，还可以访问当
前的时间和状态信息。这里的时间就包括了处理时间（processing time）和事件时间水位线（event
time watermark）。
 */
class UrlViewCountResult extends ProcessWindowFunction[Long,UrlViewCount,String,TimeWindow] {
  // 迭代器中只有一个元素，是增量聚合函数在窗口闭合时发送过来的计算结果
  override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(
      key,
      elements.iterator.next(),
      context.window.getStart,
      context.window.getEnd
    ))
  }

}
