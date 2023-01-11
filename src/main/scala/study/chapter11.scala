package study

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object chapter11 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .process(new myProcessFunction)
      .print()
    env.execute("ProcessFunction")
  }
}
class myProcessFunction extends ProcessFunction[Event,String]{
  // 每来一条元素都会调用一次
  override def processElement(value: Event, ctx: ProcessFunction[Event, String]#Context, out: Collector[String]): Unit = {
    if (value.user.equals("Mary")){
      out.collect(value.user)
    }else if (value.user.equals("Bob"))
      {
        out.collect(value.user)
        out.collect(value.user)
      }
    // 打印当前水位线
      println(ctx.timerService.currentWatermark())
  }
}