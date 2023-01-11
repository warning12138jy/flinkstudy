package State

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._

import java.util



case class LoginEvent(userId:String,ipAddress: String,eventType:String,timestamp:Long)
object chapter07 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.fromElements(
      LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
      LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
      LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
      LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
      LoginEvent("user_2", "192.168.1.29", "success", 6000L),
      LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
      LoginEvent("user_2", "192.168.1.29", "fail", 8000L)
    )
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.userId)
    // 1. 定义 Pattern，连续的三个登录失败事件
    val pattern = Pattern
      .begin[LoginEvent]("first")
      .where(_.eventType.equals("fail"))
      .next("second")
      .where(_.eventType.equals("fail"))
      .next("third")
      .where(_.eventType.equals("fail"))
    // 2. 将 Pattern 应用到流上，检测匹配的复杂事件，得到一个 PatternStream
    val patternStream = CEP.pattern(stream,pattern)
    // 3. 将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出
    patternStream.select(new PatternSelectFunction[LoginEvent,String] {
      override def select(map: util.Map[String, util.List[LoginEvent]]): String = {
        val first = map.get("first").get(0)
        val second = map.get("second").get(0)
        val third = map.get("third").get(0)
        first.userId + " 连续三次登录失败！登录时间：" + first.timestamp + ", " + second.timestamp + ", " + third.timestamp
      }
    }).print("warning")


    env.execute("flinkCEP")
  }

}
