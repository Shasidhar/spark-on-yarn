package com.madhukaraphatak.yarnexamples.task

import java.io.{ObjectOutputStream, File}
import java.util.{UUID, Collections}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records.{ContainerLaunchContext, _}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Created by madhu on 16/12/14.
 */
object Client {

  def setUpAppMasterJar(jarPath: Path, appMasterJar: LocalResource)(implicit conf:Configuration) = {
    val jarStat = FileSystem.get(conf).getFileStatus(jarPath)
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath))
    appMasterJar.setSize(jarStat.getLen())
    appMasterJar.setTimestamp(jarStat.getModificationTime())
    appMasterJar.setType(LocalResourceType.FILE)
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC)
  }

  def setUpEnv(appMasterEnv: mutable.Map[String, String])(implicit conf:YarnConfiguration) = {
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

    implicit val conf = new YarnConfiguration()
    val command = args(0)
    val jarPath = args(1)

    val client = YarnClient.createYarnClient()
    client.init(conf)
    client.start()

    println("inside the client")

    val app = client.createApplication()


    val functionalTask = new FunctionTask[Unit]( () => println("helloworld hai"))
    val fileSystem = FileSystem.get(conf)


    val taskFilePath = "/tasks/"+UUID.randomUUID().toString
    val hdfsFile = fileSystem.create(new Path(taskFilePath))
    val out = new ObjectOutputStream(hdfsFile)
    out.writeObject(functionalTask)
    IOUtils.closeStream(out)
    IOUtils.closeStream(hdfsFile)


    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setCommands(List(
      "$JAVA_HOME/bin/java" +
      " -Xmx256M" +
      " com.madhukaraphatak.yarnexamples.task.ApplicationMaster" +
        " " + taskFilePath +"  "+ jarPath+" "+
      " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
      " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
    ).asJava)


    val appMasterJar = Records.newRecord(classOf[LocalResource])
    setUpAppMasterJar(new Path(jarPath),appMasterJar)


    amContainer.setLocalResources(Collections.singletonMap("helloworld.jar",appMasterJar))

    val env = collection.mutable.Map[String,String]()
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

  }

}
