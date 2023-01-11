package State

import org.apache.flink.cep.{PatternFlatSelectFunction, PatternSelectFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util
import java.util.{Calendar, Random}



case class LoginEvent(userId:String,ipAddress: String,eventType:String,timestamp:Long)
object chapter07 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new LoginEv)
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
//    val patternStream = CEP.pattern(stream,pattern)
//    // 3. 将匹配到的复杂事件选择出来，然后包装成字符串报警信息输出
//    patternStream.select(new PatternSelectFunction[LoginEvent,String] {
//      override def select(map: util.Map[String, util.List[LoginEvent]]): String = {
//        val first = map.get("first").get(0)
//        val second = map.get("second").get(0)
//        val third = map.get("third").get(0)
//        first.userId + " 连续三次登录失败！登录时间：" + first.timestamp + ", " + second.timestamp + ", " + third.timestamp
//      }
//    }).print("warning")
    //  PatternSelectFunction 的“扁平化”版本PatternFlatSelectFunction
    val patternStream = CEP.pattern(stream,pattern)
    patternStream.flatSelect(new PatternFlatSelectFunction[LoginEvent,String] {
      override def flatSelect(map: util.Map[String, util.List[LoginEvent]], collector: Collector[String]): Unit = {
        val first = map.get("first").get(0)
        val second = map.get("second").get(0)
        val third = map.get("third").get(0)
        collector.collect((first.userId + " 连续三次登录失败！登录时间：" + first.timestamp + ", " + second.timestamp + ", " + third.timestamp))
      }
    }).print("warning")


    env.execute("flinkCEP")
  }

}
class LoginEv extends SourceFunction[LoginEvent]{
  // 标志位，用来控制循环的退出
  var running = true

  override def run(ctx: SourceFunction.SourceContext[LoginEvent]): Unit = {
    // 实例化一个随机数发生器
    val random = new Random
    // 供随机选择的用户名的数组
    val users = Array("user_1", "user_2")
    // 供随机选择的 ipAddress 的数组
    val urls = Array("192.168.0.1", "192.168.0.2", "192.168.1.29", "192.168.1.29", "171.56.23.10")
    //供随机选择的事件类型
    val eventType = Array("fail","success")
    //通过 while 循环发送数据，running 默认为 true，所以会一直发送数据
    while (running) {
      // 调用 collect 方法向下游发送数据
      ctx.collect(
        LoginEvent(
          users(random.nextInt(users.length)), // 随机选择一个用户名
          urls(random.nextInt(urls.length)), // 随机选择一个 url
          eventType(random.nextInt(eventType.length)),
          Calendar.getInstance.getTimeInMillis
        ) // 当前时间戳
      )
      // 隔 1 秒生成一个点击事件，方便观测
      Thread.sleep(1000)
    }
  }

  //通过将 running 置为 false 终止数据发送循环
  override def cancel(): Unit = {
    running = false
  }
}