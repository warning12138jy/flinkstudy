package study

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object chapter14 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // TODO:  我们可以实现一个实时对账的需求，也就是
    //  app 的支付操作和第三方的支付操作的一个双流 join。App 的支付事件和第三方的支付事件将
    //  会互相等待 5 秒钟，如果等不来对应的支付事件，那么就输出报警信息。
    //来自app的支付日志
    val appStream = env.fromElements(
      ("order-1", "app", 1000L),
      ("order-2", "app", 2000L)
    )
      .assignAscendingTimestamps(_._3)

    //来自第三方支付平台的支付日志
    val thirdPartyStream = env.fromElements(
      ("order-1", "third-party", "success", 3000L),
      ("order-3", "third-party", "success", 4000L)
    )
      .assignAscendingTimestamps(_._4)

    // 检测同一支付单在两条流中是否匹配，不匹配就报警
    appStream.connect(thirdPartyStream)
      .keyBy(_._1,_._1)
      .process(new OrderMatchResult)
      .print()

    env.execute("CoProcessFunction")
  }

}
// 自定义实现 CoProcessFunction
class OrderMatchResult extends CoProcessFunction[(String,String,Long),(String, String, String, Long),String]{
  // 定义状态变量，用来保存已经到达的事件；使用 lazy 定义是一种简洁的写法
  lazy val appEvent: ValueState[(String, String, Long)] = getRuntimeContext.getState(
    new ValueStateDescriptor[(String,String,Long)]("app",classOf[(String,String,Long)])
  )
  lazy val thirdEvent: ValueState[(String, String, String, Long)] = getRuntimeContext.getState(
    new ValueStateDescriptor[(String,String,String,Long)]("third",classOf[(String,String,String,Long)])
  )

  override def processElement1(value: (String, String, Long), ctx: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#Context, out: Collector[String]): Unit = {
    if (thirdEvent.value() != null)
    // 如果对应的第三方支付事件的状态变量不为空，则说明第三方支付事件先到达，对账成功
    {
      out.collect(value._1+"对账成功")
      // 清空保存第三方支付事件的状态变量
      thirdEvent.clear()
    }else{
      // 如果是 app 支付事件先到达，就把它保存在状态中
      appEvent.update(value)
      // 注册 5 秒之后的定时器，也就是等待第三方支付事件 5 秒钟
      ctx.timerService.registerEventTimeTimer(value._3+500L)
    }
  }
  // 和上面的逻辑是对称的关系
  override def processElement2(value: (String, String, String, Long), ctx: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#Context, out: Collector[String]): Unit = {
    if (appEvent.value() != null){
      out.collect(value._1+"对账成功")
      appEvent.clear()
    }else{
      thirdEvent.update(value)
      ctx.timerService.registerEventTimeTimer(value._4+500L)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[(String, String, Long), (String, String, String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
    // 如果 app 事件的状态变量不为空，说明等待了 5 秒钟，第三方支付事件没有到达
    if (appEvent.value() != null){
      out.collect(appEvent.value()._1+"对账失败，订单的第三方支付信息未到")
      appEvent.clear()
    }
    // 如果第三方支付事件没有到达，说明等待了 5 秒钟，app 事件没有到达
    if (thirdEvent.value() != null){
      out.collect(thirdEvent.value()._1+"对账失败，订单的 app 支付信息未到")
      thirdEvent.clear()
    }
  }
}