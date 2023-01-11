package State

import org.apache.flink.cep.functions.{PatternProcessFunction, TimedOutPartialMatchHandler}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.util



// TODO:处理超时事件


case class OrderEvent(userId: String, orderId: String, eventType: String, timestamp: Long)
object chapter09 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 获取订单事件流，并提取时间戳、生成水位线
    val stream = env.fromElements(
      OrderEvent("user_1", "order_1", "create", 1000L),
      OrderEvent("user_2", "order_2", "create", 2000L),
      OrderEvent("user_1", "order_1", "modify", 10 * 1000L),
      OrderEvent("user_1", "order_1", "pay", 60 * 1000L),
      OrderEvent("user_2", "order_3", "create", 10 * 60 * 1000L),
      OrderEvent("user_2", "order_3", "pay", 20 * 60 * 1000L)
    )
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.orderId) //按照 订单id分组

    val pattern = Pattern
      .begin[OrderEvent]("create")
      .where(_.eventType.equals("create"))
      .followedBy("pay")//在下单和支付之间，可以有其他操作（比如对订单的修改），所以两者之间是宽松近邻关系
      .where(_.eventType.equals("pay"))
      .within(Time.minutes(15))

    val patternStrem = CEP.pattern(stream,pattern)
    val paydOrderStream = patternStrem.process(new OrderTimeoutDetect)
    paydOrderStream.print("payed")
    paydOrderStream.getSideOutput(new OutputTag[String]("timeout")).print("timeout")
    env.execute("OrderTimeoutDetect")
  }

}
//使用 PatternProcessFunction 的侧输出流结合TimedOutPartialMatchHandler
class OrderTimeoutDetect extends PatternProcessFunction[OrderEvent,String] with TimedOutPartialMatchHandler[OrderEvent]{
  override def processMatch(map: util.Map[String, util.List[OrderEvent]], ctx: PatternProcessFunction.Context, out: Collector[String]): Unit = {
    val payEvent = map.get("pay").get(0)
    out.collect("订单 " + payEvent.orderId + " 已支付！")
  }

  override def processTimedOutMatch(map: util.Map[String, util.List[OrderEvent]], ctx: PatternProcessFunction.Context): Unit = {
    val createEvent = map.get("create").get(0)
    ctx.output(new OutputTag[String]("timeout"), "订单 " + createEvent.orderId + " 超时未支付！用户为：" + createEvent.userId)
  }
}