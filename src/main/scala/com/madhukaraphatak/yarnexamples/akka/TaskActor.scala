package com.madhukaraphatak.yarnexamples.akka

import akka.actor._
import com.madhukaraphatak.yarnexamples.akka.Client.RecieveActor
import com.typesafe.config.ConfigFactory


/**
 * Created by madhu on 19/12/14.
 */
/*class TaskActor(actorRef:ActorRef) extends Actor{


  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    actorRef ! RecieveActor.Hi
  }

  override def receive: Receive = {
    case _ => context.stop(self
    )
  }
}*/
object TaskActor {

  //def props(actorRef:ActorRef):Props = Props(new TaskActor(actorRef))

  def main(args: Array[String]) {
   /* val config = ConfigFactory.parseFile(new File(this.getClass.getClassLoader.
      getResource("local_application.conf").getFile))*/

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

    println("config is" + config)
    val actorSystem = ActorSystem.create("actor_system_yarn_task",config)
    val actorRef = actorSystem.actorSelection("akka.tcp://actorSystemYarn@127.0.0.1:5150/user/receiveActor")
    println(actorRef)
    actorRef ! RecieveActor.Hi

  }
}
