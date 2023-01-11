package State
import org.apache.flink.api.common.functions.{AggregateFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import study.{ClickSource, Event}

import java.sql.Timestamp
object chapter04 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 统计每个用户的点击频次，到达 5 次就输出统计结果
    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user)
      .flatMap(new AvgTsResult)
      .print()




    env.execute("AggregatingState")
  }
}
// 自定义 RichFlatMapFunction
class AvgTsResult extends RichFlatMapFunction[Event,String]{
  // 定义聚合状态，用来计算平均时间戳，中间累加器保存一个(sum, count)二元组
  lazy val avgTsAggState: AggregatingState[Event, Long] = getRuntimeContext.getAggregatingState(
    new AggregatingStateDescriptor[Event,(Long,Long),Long]("avg-ts",new AggregateFunction[Event,(Long,Long),Long] {
      override def createAccumulator(): (Long, Long) = {
        (0L,0L)
      }

      override def add(value: Event, accumulator: (Long, Long)): (Long, Long) = {
        (accumulator._1 +value.timestamp,accumulator._2+1)
      }

      override def getResult(accumulator: (Long, Long)): Long = {
        accumulator._1/accumulator._2
      }// 增量聚合函数的定义，定义了聚合的逻辑
      override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = ???
    }// 增量聚合函数的定义，定义了聚合的逻辑
       ,classOf[(Long,Long)]// 累加器的类型
    )
  )
  // 定义一个值状态，用来保存当前用户访问频次
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("count",classOf[Long])
  )

  override def flatMap(value: Event, out: Collector[String]): Unit = {
    //更新count值
    val count = countState.value()
    countState.update(count+1)
    avgTsAggState.add(value)
    // 达到 5 次就输出结果，并清空状态
    if (count==5){
      out.collect(value.user + " 平均时间戳： " + new Timestamp(avgTsAggState.get()))
      avgTsAggState.clear()
    }
  }

}