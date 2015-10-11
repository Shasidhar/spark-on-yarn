package com.madhukaraphatak.yarnexamples.customtasks

import java.io.{FileInputStream, ObjectInputStream}

import akka.actor._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger


/**
 * Created by madhu on 19/12/14.
 */

class TaskResultSenderActor(remoteActor:ActorSelection) extends Actor{

  import TaskResultSender._
  override def receive: Receive = {
    case taskResult @ TaskResult(id,result) => {
      println("sending the result to" + remoteActor)
      remoteActor ! taskResult
    }

    case TaskResultRecieved => {
      context.stop(self)
      context.system.shutdown()
    }
  }
}

object TaskResultSender{
  case object TaskResultRecieved extends Serializable
  def props(remoteActor:ActorSelection):Props = Props(new TaskResultSenderActor(remoteActor))
}

object TaskExecutor {

  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {
    val taskId = args(0)
    val remoteHostName = args(1)
    val remotePort = args(2)
    val configString =
      """
        akka {
          loglevel = "INFO"
          actor {
            provider = "akka.remote.RemoteActorRefProvider"
          }
          remote {
            enabled-transports = ["akka.remote.netty.tcp"]
            netty.tcp {
              hostname = "127.0.0.1"
              port = 0
            }
            log-sent-messages = on
            log-received-messages = on
          }
        }"""
    val config = ConfigFactory.parseString(configString)
    logger.info("config is" + config)

    val actorSystem = ActorSystem.create("actor_system_yarn_task",config)
    logger.info("remote information is"+ remoteHostName)
    logger.info("port =" + remotePort)

    val remoteUrl = "akka.tcp://actorSystemYarn@"+remoteHostName+":"+remotePort +
    "/user/receiveActor"

    logger.info("remote url = " + remoteUrl)

    val actorRef = actorSystem.actorSelection(remoteUrl)
    val taskResultSendActor = actorSystem.actorOf(TaskResultSender.props(actorRef))

    //let's run task

    val file = "taskFile"
    val inputStream = new ObjectInputStream(new FileInputStream(file))
    val task = inputStream.readObject().asInstanceOf[Task[_]]
    val result = task.run
    taskResultSendActor ! new TaskResult(taskId,result)

    //actorSystem.shutdown()

  }
}
