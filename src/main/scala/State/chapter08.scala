package State

import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util
case class LoginEv(userId: String, ipAddress: String, eventType: String, timestamp: Long)
object chapter08 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(
      LoginEv("user_1", "192.168.0.1", "fail", 2000L),
      LoginEv("user_1", "192.168.0.2", "fail", 3000L),
      LoginEv("user_2", "192.168.1.29", "fail", 4000L),
      LoginEv("user_1", "171.56.23.10", "fail", 5000L),
      LoginEv("user_2", "192.168.1.29", "success", 6000L),
      LoginEv("user_2", "192.168.1.29", "fail", 7000L),
      LoginEv("user_2", "192.168.1.29", "fail", 8000L)
    ).assignAscendingTimestamps(_.timestamp)
      .keyBy(_.userId)
//    val pattern =  Pattern
//      .begin[LoginEv]("first")
//      .where(_.eventType.equals("fail"))
//      .next("second")
//      .where(_.eventType.equals("fail"))
//      .next("third")
//      .where(_.eventType.equals("fail"))
//
//    val patternStream = CEP.pattern(stream,pattern)
//    patternStream.process(new PatternProcessFunction[LoginEv,String] {
//      override def processMatch(map: util.Map[String, util.List[LoginEv]], context: PatternProcessFunction.Context, collector: Collector[String]): Unit = {
//        val first = map.get("first").get(0)
//        val second = map.get("second").get(0)
//        val third = map.get("third").get(0)
//        collector.collect(first.userId + " 连续三次登录失败！登录时间：" + first.timestamp + ", " + second.timestamp + ", " + third.timestamp)
//      }
//    }).print("warning")

    //consecutive()//为循环模式中的匹配事件增加严格的近邻条件
    //为循环模式中的匹配事件增加严格的近邻条件，保证所有匹配事件是严格连续的。也就是说，一旦中间出现了不匹配的事件，当前循环检测就会终止。
    //这样显得更加简洁；而且即使要扩展到连续 100 次登录失败，也只需要改动一个参数而已。
    val pattern = Pattern
  .begin[LoginEv]("fails")
  .where(_.eventType.equals("fail")).times(3).consecutive()

    val patternStream = CEP.pattern(stream,pattern)
    patternStream.process(new PatternProcessFunction[LoginEv,String] {
      override def processMatch(map: util.Map[String, util.List[LoginEv]], ctx: PatternProcessFunction.Context, out: Collector[String]): Unit = {
        // 只有一个模式，匹配到了 3 个事件，放在 List 中
        val first = map.get("fails").get(0)
        val second = map.get("fails").get(1)
        val third = map.get("fails").get(2)
        out.collect(first.userId + " 连续三次登录失败！登录时间：" + first.timestamp + ", " + second.timestamp + ", " + third.timestamp)
      }
    }).print("warning")
    env.execute("flinCEP2")
  }

}
