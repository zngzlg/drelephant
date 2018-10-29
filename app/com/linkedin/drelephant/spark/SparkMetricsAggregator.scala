/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark

import com.linkedin.drelephant.analysis.{HadoopAggregatedData, HadoopApplicationData, HadoopMetricsAggregator}
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData
import com.linkedin.drelephant.math.Statistics
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.util.MemoryFormatUtils
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

import scala.util.Try


class SparkMetricsAggregator(private val aggregatorConfigurationData: AggregatorConfigurationData)
  extends HadoopMetricsAggregator {

  import SparkMetricsAggregator._

  private val logger: Logger = Logger.getLogger(classOf[SparkMetricsAggregator])
  private val trace = Logger.getLogger("TRACE")

  private val allocatedMemoryWasteBufferPercentage: Double =
    Option(aggregatorConfigurationData.getParamMap.get(ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE_KEY))
      .flatMap { value => Try(value.toDouble).toOption }
      .getOrElse(DEFAULT_ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE)

  private val hadoopAggregatedData: HadoopAggregatedData = new HadoopAggregatedData()

  override def getResult(): HadoopAggregatedData = hadoopAggregatedData

  override def aggregate(data: HadoopApplicationData): Unit = data match {
    case (data: SparkApplicationData) => {
      aggregate(data)
    }
    case _ => throw new IllegalArgumentException("data should be SparkApplicationData")
  }

  private def aggregate(data: SparkApplicationData): Unit = {
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConfigurationProperties

    def getProperty(key: String): Option[String] = appConfigurationProperties.get(key)

    lazy val isDynamicAllocationEnabled: Option[Boolean] = Some(getProperty(SPARK_DYNAMIC_ALLOCATE).exists(_.toBoolean == true))

    val executorInstances = executorInstancesOf(data)
    val executorCores = executorCoresOf(data).getOrElse(1)
    val executorMemoryBytes: Long = executorMemoryBytesOf(data).getOrElse(1024 * 1024 * 1024)
    val applicationDurationMillis = applicationDurationMillisOf(data)
    if (applicationDurationMillis < 0) {
      trace.warn(s"applicationDurationMillis is negative. Skipping Metrics Aggregation:${applicationDurationMillis}")
    } else {
      val totalExecutorTaskTimeMillis = totalExecutorTaskTimeMillisOf(data)
      // TODO:dynamic allocate for executor memory
      // val resourcesAllocatedForUse = aggregateresourcesAllocatedForUse(executorInstances, executorMemoryBytes, applicationDurationMillis)
      val resourcesAllocatedForUse = executorDynamicAllocatedForUse(data, executorMemoryBytes, executorCores)
      trace.info(s"${data.appId} resourcesAllocatedForUse: ${resourcesAllocatedForUse} cmbs")
      // val resourcesActuallyUsed = aggregateresourcesActuallyUsed(executorMemoryBytes, totalExecutorTaskTimeMillis)
      val resourceActuallyUsed = executorDynamicActualUsed(data, executorMemoryBytes)
      trace.info(s"${data.appId} resourceActuallyUsed: ${resourceActuallyUsed} cmbs")
      val resourcesWasted = executorNonDynamicWasted(resourcesAllocatedForUse,resourceActuallyUsed,executorCores)
      trace.info(s"${data.appId} resourcesWasted: ${resourcesWasted} mbs")
      // val resourcesActuallyUsedWithBuffer = resourcesActuallyUsed.doubleValue() * (1.0 + allocatedMemoryWasteBufferPercentage)
      val resourcesWastedMBSeconds = resourcesWasted
      //allocated is the total used resource from the cluster.
      if (resourcesAllocatedForUse.isValidLong) {
        hadoopAggregatedData.setResourceUsed(math.max(resourcesAllocatedForUse.toLong,0))
      } else {
        logger.warn(s"resourcesAllocatedForUse/resourcesWasted exceeds Long.MaxValue")
        logger.warn(s"ResourceUsed: ${resourcesAllocatedForUse}")
        logger.warn(s"executorInstances: ${executorInstances}")
        logger.warn(s"executorMemoryBytes:${executorMemoryBytes}")
        logger.warn(s"applicationDurationMillis:${applicationDurationMillis}")
        logger.warn(s"totalExecutorTaskTimeMillis:${totalExecutorTaskTimeMillis}")
        logger.warn(s"resourcesWastedMBSeconds:${resourcesWastedMBSeconds}")
        logger.warn(s"allocatedMemoryWasteBufferPercentage:${allocatedMemoryWasteBufferPercentage}")
      }
      hadoopAggregatedData.setResourceWasted(math.max(resourcesWastedMBSeconds.toLong,0))
    }
  }

  private def aggregateresourcesActuallyUsed(executorMemoryBytes: Long, totalExecutorTaskTimeMillis: BigInt): BigInt = {
    val bytesMillis = BigInt(executorMemoryBytes) * totalExecutorTaskTimeMillis
    (bytesMillis / (BigInt(FileUtils.ONE_MB) * BigInt(Statistics.SECOND_IN_MS)))
  }

  private def aggregateresourcesAllocatedForUse(
                                                 executorInstances: Int,
                                                 executorMemoryBytes: Long,
                                                 applicationDurationMillis: Long
                                               ): BigInt = {
    val bytesMillis = BigInt(executorInstances) * BigInt(executorMemoryBytes) * BigInt(applicationDurationMillis)
    (bytesMillis / (BigInt(FileUtils.ONE_MB) * BigInt(Statistics.SECOND_IN_MS)))
  }

  private def executorDynamicAllocatedForUse(data: SparkApplicationData, executorMem: Long, executorCore: Int): BigInt = {
    data.executorSummaries.map(l => BigInt(l.removedTime - l.addedTime) * BigInt(executorMem * executorCore)).sum / (BigInt(FileUtils.ONE_MB) * BigInt(Statistics.SECOND_IN_MS))
  }

  private def executorDynamicActualUsed(data: SparkApplicationData, executorMem: Long): BigInt = {
    data.executorSummaries.map(l => BigInt(l.totalDuration) * BigInt(executorMem)).sum / (BigInt(FileUtils.ONE_MB) * BigInt(Statistics.SECOND_IN_MS))
  }

  // storage 浪费量
  private def executorNonDynamicWasted(allocate: BigInt, used: BigInt, cores: Int): BigInt = {
    //TODO: 分配资源浪费量, 分配量 - 使用量，单位 MB  * second / core
    //分配量： (executorEndtime - executorStarttime) * executorMem * executorCore
    //使用量： executorTotalDuration * executorMem
    //算法特点：带入了核数，带入了动态或非动态分配时，executor 的idle时间
    (allocate - used) / BigInt(cores)
  }

  private def executorInstancesOf(data: SparkApplicationData): Option[Int] = {
    val appConfigurationProperties = data.appConfigurationProperties
    appConfigurationProperties.get(SPARK_EXECUTOR_INSTANCES_KEY).map(_.toInt)
  }

  private def executorMemoryBytesOf(data: SparkApplicationData): Option[Long] = {
    val appConfigurationProperties = data.appConfigurationProperties
    appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY_KEY).map(MemoryFormatUtils.stringToBytes)
  }

  private def executorCoresOf(data: SparkApplicationData): Option[Int] = {
    val appConfigurationProperties = data.appConfigurationProperties
    appConfigurationProperties.get(SPARK_EXECUTOR_CORE_KEY).map(_.toInt)
  }

  private def applicationDurationMillisOf(data: SparkApplicationData): Long = {
    require(data.applicationInfo.attempts.nonEmpty)
    val lastApplicationAttemptInfo = data.applicationInfo.attempts.last
    lastApplicationAttemptInfo.endTime.getTime - lastApplicationAttemptInfo.startTime.getTime
  }

  private def totalExecutorTaskTimeMillisOf(data: SparkApplicationData): BigInt = {
    data.executorSummaries.map { executorSummary => BigInt(executorSummary.totalDuration) }.sum
  }
}

object SparkMetricsAggregator {
  /** The percentage of allocated memory we expect to waste because of overhead. */
  val DEFAULT_ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE = 0.5D
  val ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE_KEY = "allocated_memory_waste_buffer_percentage"
  val SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances"
  val SPARK_EXECUTOR_MEMORY_KEY = "spark.executor.memory"
  val SPARK_EXECUTOR_CORE_KEY = "spark.executor.cores"
  val SPARK_DYNAMIC_ALLOCATE = "spark.dynamicAllocation.enabled"
}
