package com.madhukaraphatak.yarnexamples.task

/**
 * Created by madhu on 1/10/14.
 */

trait Task[T] extends Serializable{
  def run: T

}
class  FunctionTask[T]( body: => () => T) extends Task[T] {
  override def run: T = body()
}
