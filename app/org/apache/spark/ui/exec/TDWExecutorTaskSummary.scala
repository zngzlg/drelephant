package org.apache.spark.ui.exec

private[ui] case class TDWExecutorTaskShuffleMetrics(
  var shuffleWait:Long = 0L,
  var totalFetchBlock:Long =0L,
  var shuffleRead: Long = 0L,
  var shuffleWrite: Long = 0L
)

private[ui] case class TDWExecutorTaskSummary(
  var executorId: String,
  var totalCores: Int = 0,
  var tasksActive: Int = 0,
  var tasksFailed: Int = 0,
  var tasksComplete: Int = 0,
  var duration: Long = 0L,
  var jvmGCTime: Long = 0L,
  var inputBytes: Long = 0L,
  var inputRecords: Long = 0L,
  var isAlive: Boolean = true,
  var addedTime: Long = 0L, // Executor 加入时间
  var removedTime: Long = 0L, // Executor 移除时间
  var deserializationTime: Long = 0L, // Executor 在做task反序列化时的耗时，也就是task的依赖准备阶段
  var executorCpuTime: Long = 0L,
  var executorRuntime: Long = 0L,
  var executorSerializationTime: Long = 0L,
  var shuffleMetrics: TDWExecutorTaskShuffleMetrics = TDWExecutorTaskShuffleMetrics()
)
