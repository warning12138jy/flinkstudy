package State

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import study.{ClickSource, Event}

object chapter01 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // TODO:我们这里会使用用户 id 来进行分流，然后分别统计每个用户的 pv 数据，由于我们并不想
    //每次 pv 加一，就将统计结果发送到下游去，所以这里我们注册了一个定时器，用来隔一段时
    //间发送 pv 的统计结果，这样对下游算子的压力不至于太大。具体实现方式是定义一个用来保
    //存定时器时间戳的值状态变量。当定时器触发并向下游发送数据以后，便清空储存定时器时间
    //戳的状态变量，这样当新的数据到来时，发现并没有定时器存在，就可以注册新的定时器了，
    //注册完定时器之后将定时器的时间戳继续保存在状态变量中。

    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.user) //按照用户分组
      .process(new PeriodicPvResult) //自定义KeyedProcessFunction
      .print()

  env.execute("ValueState")
  }

}

// 注册定时器，周期性输出 pv
class PeriodicPvResult extends KeyedProcessFunction[String,Event,String]{
  // 懒加载值状态变量，用来储存当前 pv 数据
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("count",classOf[Long])
  )
  // 懒加载状态变量，用来储存发送 pv 数据的定时器的时间戳
  lazy val timeState: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("time-ts",classOf[Long])
  )

  override def processElement(value: Event, ctx: KeyedProcessFunction[String, Event, String]#Context, out: Collector[String]): Unit = {
    //更新count值
    val count = countState.value()
    countState.update(count+1)
    // 如果保存发送 pv 数据的定时器的时间戳的状态变量为 0L，则注册一个 10 秒后的定时器
    if (timeState.value() ==0L){
      ctx.timerService.registerEventTimeTimer(value.timestamp+10*1000L)
      // 将定时器的时间戳保存在状态变量中
      timeState.update(value.timestamp+10*1000L)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 定时器触发，向下游输出当前统计结果
    out.collect("用户 " + ctx.getCurrentKey + " 的 pv 是：" + countState.value())
    // 清空保存定时器时间戳的状态变量，这样新数据到来时又可以注册定时器了
    timeState.clear()
  }
}