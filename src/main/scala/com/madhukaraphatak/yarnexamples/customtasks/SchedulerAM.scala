package com.madhukaraphatak.yarnexamples.customtasks

import java.io.File
import java.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.NMClient
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records}

import scala.collection.JavaConverters._

/**
 * Created by madhu on 16/12/14.
 */
class SchedulerAM(taskFilePaths:Array[String],jarPath:String,hostName:String,port:Int) extends AMRMClientAsync.CallbackHandler{

  val conf = new YarnConfiguration()
  val nmClient = NMClient.createNMClient()
  nmClient.init(conf)
  nmClient.start()
  var completed = 0
  var allocatedTasks = 0

  val appMasterJar = Records.newRecord(classOf[LocalResource])
  setUpAppMasterJar(new Path(jarPath),appMasterJar)

  def setUpEnv(appMasterEnv: collection.mutable.Map[String, String]) = {
    val classPath = YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH

    for (c <- classPath){
      Apps.addToEnvironment(appMasterEnv.asJava, Environment.CLASSPATH.name(),
        c.trim())
    }
    Apps.addToEnvironment(appMasterEnv.asJava,
      Environment.CLASSPATH.name(),
      Environment.PWD.$() + File.separator + "*")

  }

  def setUpAppMasterJar(jarPath: Path, appMasterJar: LocalResource) = {
    val jarStat = FileSystem.get(conf).getFileStatus(jarPath)
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarStat.getPath))
    appMasterJar.setSize(jarStat.getLen())
    appMasterJar.setTimestamp(jarStat.getModificationTime())
    appMasterJar.setType(LocalResourceType.FILE)
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC)
  }


  override def onContainersCompleted(statuses: util.List[ContainerStatus]): Unit = {
    for ( status <- statuses.asScala){
      println("completed"+status.getContainerId)
      completed+=1
    }
  }

  override def onError(e: Throwable): Unit = {


  }

  override def getProgress: Float = {
    return 0.0f
  }

  override def onShutdownRequest(): Unit = {



  }

  override def onNodesUpdated(updatedNodes: util.List[NodeReport]): Unit = {

  }

  override def onContainersAllocated(containers: util.List[Container]): Unit = {
    for (container <- containers.asScala) {

      //this file for task
      val taskFileResource = Records.newRecord(classOf[LocalResource])
      val taskFile  = new Path(taskFilePaths(allocatedTasks))
      setUpAppMasterJar(taskFile,taskFileResource)

      val taskId = taskFile.getName

      val ctx =
        Records.newRecord(classOf[ContainerLaunchContext])
      ctx.setCommands(
        List(
          "$JAVA_HOME/bin/java" +
            " -Xmx256M" +
            " com.madhukaraphatak.yarnexamples.customtasks.TaskExecutor" +
             "  "+ taskId +"  "+ hostName + "  "+ port+"  "+
            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
        ).asJava
      )


      val localResources = collection.mutable.Map[String,LocalResource]()
      localResources += "helloworld.jar" -> appMasterJar
      localResources += "taskFile" -> taskFileResource

      ctx.setLocalResources(localResources.asJava)

      val env = collection.mutable.Map[String,String]()
      setUpEnv(env)

      ctx.setEnvironment(env.asJava)

      System.out.println("Launching container " + container)
      allocatedTasks+=1
      nmClient.startContainer(container, ctx)
    }
  }

  def runMainLoop() = {

    val conf = new YarnConfiguration()

    val rmClient = AMRMClientAsync.createAMRMClientAsync[ContainerRequest](100,this)
    rmClient.init(conf)
    rmClient.start()
    rmClient.registerApplicationMaster("", 0, "")


    val priority = Records.newRecord(classOf[Priority])
    priority.setPriority(0)

    val resource = Records.newRecord(classOf[Resource])
    resource.setMemory(128)
    resource.setVirtualCores(1)

    for ( i <- 1 to taskFilePaths.length) {
      val containerAsk = new ContainerRequest(resource,null,null,priority)
      println("asking for " +s"$i")
      rmClient.addContainerRequest(containerAsk)
    }

    while(completed<taskFilePaths.length){
      Thread.sleep(1000)
    }

    rmClient.unregisterApplicationMaster(
      FinalApplicationStatus.SUCCEEDED, "", "")
  }

}

object SchedulerAM {
  def main(args: Array[String]) {
    val taskFilePaths = args(0).split(",")
    val jarPath = args(1)
    val hostName = args(2)
    val port = args(3).toInt
    val master = new SchedulerAM(taskFilePaths, jarPath,hostName,port)
    master.runMainLoop()
  }
}

