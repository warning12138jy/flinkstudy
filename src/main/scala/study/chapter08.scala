package study

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object chapter08 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_ => "key")
      .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)))
      .aggregate(new avgPv)
      .print()




    env.execute("AggregateFunction")
  }

}

// TODO:
//  AggregateFunction 接口中有四个方法：
//  ⚫ createAccumulator()：创建一个累加器，这就是为聚合创建了一个初始状态，每个聚合任务只会调用一次。
//  ⚫ add()：将输入的元素添加到累加器中。这就是基于聚合状态，对新来的数据进行进
//  一步聚合的过程。方法传入两个参数：当前新到的数据 value，和当前的累加器
//  accumulator；返回一个新的累加器值，也就是对聚合状态进行更新。每条数据到来之
//  后都会调用这个方法。
//  ⚫ getResult()：从累加器中提取聚合的输出结果。也就是说，我们可以定义多个状态，
//  然后再基于这些聚合的状态计算出一个结果进行输出。比如之前我们提到的计算平均
//  值，就可以把 sum 和 count 作为状态放入累加器，而在调用这个方法时相除得到最终
//  结果。这个方法只在窗口要输出结果时调用。
//  ⚫ merge()：合并两个累加器，并将合并后的状态作为一个累加器返回。这个方法只在
//  需要合并窗口的场景下才会被调用；最常见的合并窗口（Merging Window）的场景
//  就是会话窗口（Session Windows）。
class avgPv extends AggregateFunction[Event,(Set[String],Double),Double]{
  // 创建空累加器，类型是元组，元组的第一个元素类型为 Set 数据结构，用来对用户名进行去重
  // 第二个元素用来累加 pv 操作，也就是每来一条数据就加一
  override def createAccumulator(): (Set[String], Double) = {
    (Set[String](),0L)
  }
  // 累加规则
  override def add(value: Event, accumulator: (Set[String], Double)): (Set[String], Double) = {
    (accumulator._1+value.user,accumulator._2+1L)
  }
  // 获取窗口关闭时向下游发送的结果
  override def getResult(accumulator: (Set[String], Double)): Double = {
    accumulator._2/accumulator._1.size
  }
  // merge 方法只有在事件时间的会话窗口时，才需要实现，这里无需实现。
  override def merge(a: (Set[String], Double), b: (Set[String], Double)): (Set[String], Double) = ???
}