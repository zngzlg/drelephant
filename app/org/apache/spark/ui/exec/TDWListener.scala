package org.apache.spark.ui.exec

import org.apache.spark.scheduler._
import org.apache.spark.{ExceptionFailure, Resubmitted, SparkConf}

import scala.collection.mutable


class TDWListener(conf: SparkConf) extends SparkListener {

  val executorToTaskSummary = mutable.LinkedHashMap[String, TDWExecutorTaskSummary]()

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    val eid = executorAdded.executorId
    val taskSummary = executorToTaskSummary.getOrElseUpdate(eid, TDWExecutorTaskSummary(eid))
    taskSummary.addedTime = executorAdded.time
    taskSummary.totalCores = executorAdded.executorInfo.totalCores
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = synchronized {
    executorToTaskSummary.get(executorRemoved.executorId).foreach(e => {
      e.isAlive = false
      e.removedTime = executorRemoved.time
    })
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded):Unit = synchronized {
    val blockManagerId = blockManagerAdded.blockManagerId
    val executorId = blockManagerId.executorId
    val executor = executorToTaskSummary.getOrElseUpdate(executorId,TDWExecutorTaskSummary(executorId))
    if (executor.addedTime == 0){
      executor.addedTime = blockManagerAdded.time
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    val eid = taskStart.taskInfo.executorId
    val taskSummary = executorToTaskSummary.getOrElseUpdate(eid, TDWExecutorTaskSummary(eid))
    if(taskSummary.addedTime == 0) {
      taskSummary.addedTime = taskStart.taskInfo.launchTime
    }
    taskSummary.tasksActive += 1
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    if (info != null) {
      val eid = info.executorId

      val taskSummary = executorToTaskSummary.getOrElseUpdate(eid, TDWExecutorTaskSummary(eid))

      taskEnd.reason match {
        case Resubmitted =>
          // Note: For resubmitted tasks, we continue to use the metrics that belong to the
          // first attempt of this task. This may not be 100% accurate because the first attempt
          // could have failed half-way through. The correct fix would be to keep track of the
          // metrics added by each attempt, but this is much more complicated.
          return
        case e: ExceptionFailure =>
          taskSummary.tasksFailed += 1
        case _ =>
          taskSummary.tasksComplete += 1
      }
      if (taskSummary.tasksActive >= 1) {
        taskSummary.tasksActive -= 1
      }
      taskSummary.duration += info.duration

      val metrics = taskEnd.taskMetrics
      if (metrics != null) {
        taskSummary.deserializationTime += taskEnd.taskMetrics.executorDeserializeTime
        taskSummary.executorCpuTime += taskEnd.taskMetrics.executorCpuTime
        taskSummary.executorRuntime += taskEnd.taskMetrics.executorRunTime
        taskSummary.executorSerializationTime += taskEnd.taskMetrics.resultSerializationTime
        taskSummary.shuffleMetrics.shuffleRead += metrics.shuffleReadMetrics.remoteBytesRead
        taskSummary.shuffleMetrics.shuffleWrite += metrics.shuffleWriteMetrics.bytesWritten
        taskSummary.shuffleMetrics.shuffleWait += taskEnd.taskMetrics.shuffleReadMetrics.fetchWaitTime
        taskSummary.shuffleMetrics.totalFetchBlock += taskEnd.taskMetrics.shuffleReadMetrics.totalBlocksFetched
        taskSummary.inputRecords += taskEnd.taskMetrics.shuffleReadMetrics.recordsRead
      }
    }
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = synchronized {
    executorToTaskSummary.filter { m =>
      m._2.removedTime == 0
    }.foreach(l => l._2.removedTime = applicationEnd.time)

    executorToTaskSummary.filter { m =>
      m._2.addedTime == 0
    }.foreach(l => l._2.addedTime = l._2.removedTime)
  }

}
