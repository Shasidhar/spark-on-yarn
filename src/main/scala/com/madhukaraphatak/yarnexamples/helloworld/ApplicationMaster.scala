package com.madhukaraphatak.yarnexamples.helloworld

import java.io.File
import java.util.Collections

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.{AMRMClient, NMClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records}

import scala.collection.JavaConverters._

/**
 * Created by madhu on 16/12/14.
 */
object ApplicationMaster {

  def setUpAppMasterJar(jarPath: Path, appMasterJar: LocalResource)(implicit conf:Configuration) = {
    val jarStat = FileSystem.get(conf).getFileStatus(jarPath)
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath))
    appMasterJar.setSize(jarStat.getLen())
    appMasterJar.setTimestamp(jarStat.getModificationTime())
    appMasterJar.setType(LocalResourceType.FILE)
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC)
  }

  def setUpEnv(appMasterEnv: collection.mutable.Map[String, String])(implicit conf:YarnConfiguration) = {
    val classPath = YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH

    for (c <- classPath){
      Apps.addToEnvironment(appMasterEnv.asJava, Environment.CLASSPATH.name(),
        c.trim())
    }
    Apps.addToEnvironment(appMasterEnv.asJava,
      Environment.CLASSPATH.name(),
      Environment.PWD.$() + File.separator + "*")

  }


  def main(args: Array[String]) {

    val command = args(0)
    val jarPath = args(1)
    val n = 2 //args(1).toInt

    println("command is "+command)

    implicit val conf = new YarnConfiguration()

    val rmClient = AMRMClient.createAMRMClient().asInstanceOf[AMRMClient[ContainerRequest]]
    rmClient.init(conf)
    rmClient.start()
    rmClient.registerApplicationMaster("", 0, "")


    val nmClient = NMClient.createNMClient()
    nmClient.init(conf)
    nmClient.start()

    val priority = Records.newRecord(classOf[Priority])
    priority.setPriority(0)

    val resource = Records.newRecord(classOf[Resource])
    resource.setMemory(128)
    resource.setVirtualCores(1)

    for ( i <- 0 to n) {
      val containerAsk = new ContainerRequest(resource,null,null,priority)
      println("asking for " +s"$i")
      rmClient.addContainerRequest(containerAsk)
    }

    var responseId = 0
    var completedContainers = 0

    while( completedContainers < n) {

      val appMasterJar = Records.newRecord(classOf[LocalResource])
      setUpAppMasterJar(new Path(jarPath),appMasterJar)

      val env = collection.mutable.Map[String,String]()
      setUpEnv(env)


      val response = rmClient.allocate(responseId+1)
      responseId+=1
      for (container <- response.getAllocatedContainers.asScala) {
        val ctx =
          Records.newRecord(classOf[ContainerLaunchContext])
        ctx.setCommands(
          List(
            "$JAVA_HOME/bin/java" +
              " -Xmx256M" +
              " com.madhukaraphatak.yarnexaperiments.HelloWorld" +
              " " + command +
              " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
              " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
          ).asJava
        )
        ctx.setLocalResources(Collections.singletonMap("helloworld.jar",appMasterJar))
        ctx.setEnvironment(env.asJava)

        System.out.println("Launching container " + container)
        nmClient.startContainer(container, ctx)
      }

      for ( status <- response.getCompletedContainersStatuses.asScala){
        println("completed"+status.getContainerId)
        completedContainers+=1

      }

      Thread.sleep(10000)
    }

    rmClient.unregisterApplicationMaster(
      FinalApplicationStatus.SUCCEEDED, "", "")
  }

}
