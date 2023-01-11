package State

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import study.{ClickSource, Event}

import java.sql.Timestamp

object chapter03 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //// 统计每 10s 滚动窗口内，每个 url 的 pv
    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.url)
      .process(new  FakeWindowResult(10000L))
      .print()

    env.execute("MapState")
  }

}
// 自定义 KeyedProcessFunction 实现滚动窗口功能
class FakeWindowResult(windowSize: Long) extends KeyedProcessFunction[String,Event,String]{
  // 初始化一个 MapState 状态变量，key 为窗口的开始时间，value 为窗口对应的 pv 数据
  lazy val windowPvMapState: MapState[Long,Long] = getRuntimeContext.getMapState(
    new MapStateDescriptor[Long,Long]("window-pv",classOf[Long],classOf[Long])
  )

  override def processElement(value: Event, ctx: KeyedProcessFunction[String, Event, String]#Context, out: Collector[String]): Unit = {
    // 根据事件的时间戳，计算当前事件所属的窗口开始和结束时间
    val windowStart = value.timestamp / windowSize*windowSize
    val windowEnd = windowStart+windowSize
    // 注册一个 windowEnd - 1ms 的定时器，用来触发窗口计算
    ctx.timerService.registerEventTimeTimer(windowEnd-1)
    // 更新状态中的 pv 值
    if (windowPvMapState.contains(windowStart)){
      val pv = windowPvMapState.get(windowStart)
      windowPvMapState.put(windowStart,pv+1L)
    }else{
      // 如果 key 不存在，说明当前窗口的第一个事件到达
      windowPvMapState.put(windowStart,1L)
    }

  }
  // 定时器触发，直接输出统计的 pv 结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 计算窗口的结束时间和开始时间
    val windowEnd = timestamp+1L
    val windowStart = windowEnd - windowSize
    // 发送窗口计算的结果
    out.collect("url: " + ctx.getCurrentKey
      + " 访问量: " + windowPvMapState.get(windowStart)
      + " 窗口：" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd))
    // 模拟窗口的销毁，清除 map 中的 key
    windowPvMapState.remove(windowStart)
  }
}