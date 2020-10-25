package com.flink.scala.course01

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamWCScalaApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import  org.apache.flink.api.scala._
    val text = env.socketTextStream("localhost",7777)

    text.flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print().setParallelism(1)
    env.execute("SteamWCscalaApp")
  }
}
