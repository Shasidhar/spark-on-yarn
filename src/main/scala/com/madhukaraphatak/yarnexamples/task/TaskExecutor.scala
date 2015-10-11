package com.madhukaraphatak.yarnexamples.task

import java.io.{FileInputStream, ObjectInputStream}

/**
 * Created by madhu on 18/12/14.
 */
object TaskExecutor {

  def main(args: Array[String]) {
    println("running some code")
    val file = "taskFile"
    val inputStream = new ObjectInputStream(new FileInputStream(file))
    val task = inputStream.readObject().asInstanceOf[Task[_]]
    task.run
    Thread.sleep(100000)
  }
}
