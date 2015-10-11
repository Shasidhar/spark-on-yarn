package com.madhukaraphatak.yarnexamples.customtasks

/**
 * Created by madhu on 6/1/15.
 */
case class TaskResult[T](taskId:String,result:T) extends Serializable

