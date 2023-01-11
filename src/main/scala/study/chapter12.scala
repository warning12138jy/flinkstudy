package study

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.sql.Timestamp

object chapter12 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.addSource(new ClickSource)
      .keyBy(r=>true)
      .process(new myKeyedProcessFunction)
      .print()
    env.execute("KeyedProcessFunction")
  }

}
class myKeyedProcessFunction extends KeyedProcessFunction[Boolean,Event,String]{
  override def processElement(value: Event, ctx: KeyedProcessFunction[Boolean, Event, String]#Context, out: Collector[String]): Unit = {
    val currTs = ctx.timerService.currentProcessingTime()
    out.collect("数据到达，到达时间：" + new Timestamp(currTs))
    //注册10秒钟之后的处理时间定时器
    ctx.timerService.registerProcessingTimeTimer(currTs+10*100L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Boolean, Event, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(("定时器触发，触发时间：" + new Timestamp(timestamp)))
  }
}