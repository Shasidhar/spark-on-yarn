package com.madhukaraphatak.yarnexamples.customtasks

/**
 * Created by madhu on 6/1/15.
 */
object CustomTasks {


  def errorTasks() = {
    0 to 5 map {
      index => if (index % 2 == 0) {
        new FunctionTask[Unit](() => println("hi" + index))
      } else {
        new FunctionTask[Unit](() => throw new RuntimeException("something went wrong"))
      }
    }
  }

  def singleTask() = {
    List(new FunctionTask[Unit](() => println("hi")))
  }

  def sleepTask() = {
    List(new FunctionTask[Unit](() => {
      println("sleeping")
      Thread.sleep(10000)
    }))
  }

  def resultTasks() = {
    0 to 5 map {
      index => {
        val f = new FunctionTask[Int](() => index)
        f
      }
    }

  }

  def returnTasks(value: Int) = {
    val returnF = new FunctionTask[Int](
      () => value * value
    )
    new FunctionTask[FunctionTask[Int]](() => returnF)

  }

  /**
   *
   * @param args
   * args(0) - hdfs path of jar containing classes
   * Ex: hdfs://localhost:54311/jars/yarnexamples-1.0-SNAPSHOT.jar
   */

  def main(args: Array[String]) {
    val yarnScheduler = new YarnScheduler(args(0))
    println("hi shashi")
    println("running multiple tasks")
    println("result is" + yarnScheduler.runTasks(resultTasks(): _*).toList)
    /*println("running sleep tasks")
    yarnScheduler.runTasks(sleepTask(): _*)
    println("multi level tasks")
    val results = yarnScheduler.runTasks(returnTasks(10))
    println("running second level tasks "+ yarnScheduler.runTasks(results.toList:_*))*/


  }

}
