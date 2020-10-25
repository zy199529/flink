package com.flink.scala.course01

import org.apache.flink.api.scala.ExecutionEnvironment


object BatchWCScalaApp {
  def main(args: Array[String]): Unit = {
    val input = "file:///E:/flink_quickstart_java/local/kv.txt"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text=env.readTextFile(input)
    import  org.apache.flink.api.scala._
    text.flatMap(_.toLowerCase.split(' '))
      .filter(_.nonEmpty).map((_,1))
      .groupBy(0)
      .sum(1)
      .print()

  }
}
