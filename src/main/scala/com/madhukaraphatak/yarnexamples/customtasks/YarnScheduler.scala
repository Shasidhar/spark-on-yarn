package com.madhukaraphatak.yarnexamples.customtasks

import java.io.{File, ObjectOutputStream}
import java.util.{Collections, UUID}

import akka.actor._
import com.madhukaraphatak.yarnexamples.customtasks.TaskResultSender.TaskResultRecieved
import com.typesafe.config.ConfigFactory

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{ContainerLaunchContext, _}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records}
import org.apache.log4j.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 *
 * @param jarPath hdfs path of jar containing classes
 */
class YarnScheduler(jarPath: String) {

  val logger = Logger.getLogger(classOf[YarnScheduler])

  def setUpAppMasterJar(jarPath: Path, appMasterJar: LocalResource)(implicit conf: Configuration) = {
    val jarStat = FileSystem.get(conf).getFileStatus(jarPath)
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath))
    appMasterJar.setSize(jarStat.getLen())
    appMasterJar.setTimestamp(jarStat.getModificationTime())
    appMasterJar.setType(LocalResourceType.FILE)
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC)
  }

  def setUpEnv(appMasterEnv: mutable.Map[String, String])(implicit conf: YarnConfiguration) = {
    val classPath = YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH

    for (c <- classPath) {
      Apps.addToEnvironment(appMasterEnv.asJava, Environment.CLASSPATH.name(),
        c.trim())
    }
    Apps.addToEnvironment(appMasterEnv.asJava,
      Environment.CLASSPATH.name(),
      Environment.PWD.$() + File.separator + "*")

  }

  def runTasks[T](tasks: Task[T]*)(implicit classTag: ClassTag[T]): Array[T] = {

    var allFinished = false
    val allFinishedLock = new Object()
    var taskIdToIndex = mutable.Map[String, Int]()
    val taskResult = new Array[T](tasks.length)
    val totalTasks = tasks.length

    class RecieveActor() extends Actor {
      var completedTasks = 0

      @throws[Exception](classOf[Exception])
      override def postStop(): Unit = {
        context.system.shutdown()
      }

      override def receive: Receive = {
        case TaskResult(taskId, result) => {
          println("result is" + result)
          taskResult(taskIdToIndex(taskId)) = result.asInstanceOf[T]
          sender ! TaskResultRecieved
          completedTasks += 1
          if (completedTasks >= totalTasks) {
            allFinishedLock.synchronized {
              allFinished = true
              allFinishedLock.notifyAll()
              context.stop(self)
            }
          }
        }
      }
    }
    object RecieveActor {

      def props[T](totalTasks: Int, taskResult: Array[T]): Props = Props(new RecieveActor())
    }


    implicit val conf = new YarnConfiguration()
    val client = YarnClient.createYarnClient()
    client.init(conf)
    client.start()

    println("inside the client")

    val app = client.createApplication()



    val config = ConfigFactory.parseFile(new File(Thread.currentThread().getContextClassLoader.
      getResource("remote_application.conf").getFile))


    logger.debug("config is "+config)

    val actorSystem = ActorSystem.create("actorSystemYarn", config)

    val remoteAddress = actorSystem.asInstanceOf[ExtendedActorSystem].provider.getDefaultAddress

    val hostName = remoteAddress.host.get

    val port = remoteAddress.port.get

    logger.info(" hostName is = "+ hostName)

    logger.info(" port is = " + port)


    actorSystem.actorOf(RecieveActor.props(tasks.length, taskResult), name = "receiveActor")


    val taskStringBuffer = new ArrayBuffer[String](tasks.length)

    for ((task, index) <- tasks.zipWithIndex) {

      val fileSystem = FileSystem.get(conf)
      val taskId = UUID.randomUUID().toString
      val taskFilePath = "/tasks/" + taskId
      val hdfsFile = fileSystem.create(new Path(taskFilePath))
      val out = new ObjectOutputStream(hdfsFile)
      out.writeObject(task)
      IOUtils.closeStream(out)
      IOUtils.closeStream(hdfsFile)
      taskStringBuffer.+=:(taskFilePath)
      taskIdToIndex += taskId -> index
    }


    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setCommands(List(
      "$JAVA_HOME/bin/java" +
        " -Xmx256M" +
        " com.madhukaraphatak.yarnexamples.customtasks.SchedulerAM" +
        " " + taskStringBuffer.mkString(",") + "  " + jarPath + " " +
        " "+ hostName+"   "+ port+"   "+
        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
    ).asJava)


    val appMasterJar = Records.newRecord(classOf[LocalResource])
    setUpAppMasterJar(new Path(jarPath), appMasterJar)


    amContainer.setLocalResources(Collections.singletonMap("helloworld.jar", appMasterJar))

    val env = collection.mutable.Map[String, String]()
    setUpEnv(env)
    amContainer.setEnvironment(env.asJava)

    val resource = Records.newRecord(classOf[Resource])
    resource.setMemory(300)
    resource.setVirtualCores(1)

    val appContext = app.getApplicationSubmissionContext
    appContext.setApplicationName("helloworld")
    appContext.setAMContainerSpec(amContainer)
    appContext.setResource(resource)
    appContext.setQueue("default")

    val appId = appContext.getApplicationId
    println("submitting application id" + appId)
    client.submitApplication(appContext)

    allFinishedLock.synchronized {
      while (allFinished != true) {
        allFinishedLock.wait()
      }
    }
    taskResult
  }

}
