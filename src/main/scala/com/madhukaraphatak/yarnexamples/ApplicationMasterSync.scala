package com.madhukaraphatak.yarnexamples

import java.util
import java.util.Collections

import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.client.api.NMClient
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records

import scala.collection.JavaConverters._

/**
 * Created by madhu on 16/12/14.
 */
class ApplicationMasterSync(command:String,n:Int) extends AMRMClientAsync.CallbackHandler{

  val config = new YarnConfiguration()
  val nmClient = NMClient.createNMClient()
  nmClient.init(config)
  nmClient.start()
  var completed = 0

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
      val ctx =
        Records.newRecord(classOf[ContainerLaunchContext])
      ctx.setCommands(
        Collections.singletonList(
          command+
      " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
      ))
      System.out.println("Launching container " + container)
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

    for ( i <- 0 to n) {
      val containerAsk = new ContainerRequest(resource,null,null,priority)
      println("asking for " +s"$i")
      rmClient.addContainerRequest(containerAsk)
    }

    while(completed<n){
      Thread.sleep(1000)
    }

    rmClient.unregisterApplicationMaster(
      FinalApplicationStatus.SUCCEEDED, "", "")
   }

}

object ApplicationMasterSync{
  def main(args: Array[String]) {
    val command = args(0)
    val n = 2
    val master = new ApplicationMasterSync(command,n)
    master.runMainLoop()
  }



}
