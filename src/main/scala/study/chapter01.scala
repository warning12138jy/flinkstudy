package study

import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import java.util.{Calendar, Random}


case class Event(user:String,url:String,timestamp:Long)
//自定义源算子
// 实现 SourceFunction 接口，接口中的泛型是自定义数据源中的类型
class ClickSource extends SourceFunction[Event]{
  // 标志位，用来控制循环的退出
  var running = true
  override def run(ctx: SourceFunction.SourceContext[Event]): Unit = {
    // 实例化一个随机数发生器
    val random = new Random
    // 供随机选择的用户名的数组
    val users = Array("Mary", "Bob", "Alice", "Cary")
    // 供随机选择的 url 的数组
    val urls = Array("./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2")
    //通过 while 循环发送数据，running 默认为 true，所以会一直发送数据
    while (running) {
      // 调用 collect 方法向下游发送数据
      ctx.collect(
        Event(
          users(random.nextInt(users.length)), // 随机选择一个用户名
          urls(random.nextInt(urls.length)), // 随机选择一个 url
          Calendar.getInstance.getTimeInMillis // 当前时间戳
        )
      )
      // 隔 1 秒生成一个点击事件，方便观测
      Thread.sleep(1000)
    }
  }
  //通过将 running 置为 false 终止数据发送循环
    override def cancel(): Unit = {
      running=false
    }
}
object chapter01 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new ClickSource)
//      .map(r => (r.user,1L))
//    //按照用户名进行分组
//      .keyBy(_._1)
//    //计算每个用户的访问频次
//      .reduce((r1,r2)=>(r1._1,r1._2+r2._2))
//    //将所有数据都分到同一个分区
//      .keyBy(_=>true)
//      //通过 reduce 实现 max 功能，计算访问频次最高的用户
//      .reduce((r1,r2)=> if (r1._2>r2._2)r1 else r2)
//      .print()
    stream.filter(new myFilter)
      .print()

    //map()是大家非常熟悉的大数据操作算子，主要用于将数据流中的数据进行转换，形成新
    //的数据流。简单来说，就是一个“一一映射”，消费一个元素就产出一个元素
    //stream.map(_.user).print()

    //filter()转换操作，顾名思义是对数据流执行一个过滤，通过一个布尔条件表达式设置过滤
    //条件，对于每一个流内元素进行判断，若为 true 则元素正常输出，若为 false 则元素被过滤掉，
    //stream.filter(_.user.equals("Mary")).print()
    //stream.print()

    //flatMap()操作又称为扁平映射，主要是将数据流中的整体（一般是集合类型）拆分成一个
    //一个的个体使用。消费一个元素，可以产生 0 到多个元素。flatMap()可以认为是“扁平化”
    //（flatten）和“映射”（map）两步操作的结合，也就是先按照某种规则对数据进行打散拆分，
    //再对拆分后的元素做转换处理
    //stream.flatMap(new myFlatMap).print()
//    val keyedStream = stream.keyBy(_.user)
//    keyedStream.print()
    env.execute("test")
  }
}
class myFlatMap extends FlatMapFunction[Event,String]{
  override def flatMap(value: Event, out: Collector[String]): Unit = {
    //如果是 Mary 的点击事件，则向下游发送 1 次，如果是 Bob 的点击事件，则向下游发送 2 次
    if (value.user.equals("Mary")){
      out.collect(value.user)
    }else if(value.user.equals("Bob")) {
      out.collect(value.user)
      out.collect(value.user)
    }
  }
}
class myFilter extends FilterFunction[Event] {
  override def filter(value: Event): Boolean = {
    value.url.contains("home")
  }
}
