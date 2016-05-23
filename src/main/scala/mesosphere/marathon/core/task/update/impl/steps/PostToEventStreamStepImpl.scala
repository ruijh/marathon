package mesosphere.marathon.core.task.update.impl.steps

import javax.inject.Named

import akka.event.EventStream
import com.google.inject.Inject
import mesosphere.marathon.core.task.bus.MarathonTaskStatus
import mesosphere.marathon.core.task.bus.TaskChangeObservables.TaskChanged
import mesosphere.marathon.core.task.update.TaskUpdateStep
import mesosphere.marathon.core.task.{ Task, TaskStateOp }
import mesosphere.marathon.event.{ EventModule, MesosStatusUpdateEvent }
import mesosphere.marathon.state.Timestamp
import org.apache.mesos.Protos.TaskStatus
import org.slf4j.LoggerFactory

import scala.concurrent.Future

/**
  * Post this update to the internal event stream.
  */
class PostToEventStreamStepImpl @Inject() (
    @Named(EventModule.busName) eventBus: EventStream) extends TaskUpdateStep {
  private[this] val log = LoggerFactory.getLogger(getClass)

  override def name: String = "postTaskStatusEvent"

  override def processUpdate(taskChanged: TaskChanged): Future[_] = {
    taskChanged.stateOp match {
      case TaskStateOp.MesosUpdate(task, MarathonTaskStatus.WithMesosStatus(mesosStatus), timestamp) =>
        // if we receive a TaskStateOp.MesosUpdate it means someone else already decided that
        // there has been a change which needs to be persisted – we can safely decide to publish this!
        postEvent(timestamp, mesosStatus, task)

      case _ =>
      // ignore
    }

    Future.successful(())
  }

  private[this] def postEvent(timestamp: Timestamp, status: TaskStatus, task: Task): Unit = {
    val taskId = task.taskId
    task.launched.foreach { launched =>
      log.info(
        "Sending event notification for {} of app [{}]: {}",
        Array[Object](taskId, taskId.appId, status.getState): _*
      )
      eventBus.publish(
        MesosStatusUpdateEvent(
          slaveId = status.getSlaveId.getValue,
          taskId = Task.Id(status.getTaskId),
          taskStatus = status.getState.name,
          message = if (status.hasMessage) status.getMessage else "",
          appId = taskId.appId,
          host = task.agentInfo.host,
          ipAddresses = Task.MesosStatus.ipAddresses(status),
          ports = launched.hostPorts,
          version = launched.appVersion.toString,
          timestamp = timestamp.toString
        )
      )

    }
  }

}
