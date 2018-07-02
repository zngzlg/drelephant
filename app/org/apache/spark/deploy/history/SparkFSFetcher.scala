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

package org.apache.spark.deploy.history

import java.nio.charset.MalformedInputException
import java.security.PrivilegedAction
import com.linkedin.drelephant.analysis.{AnalyticJob, ElephantFetcher}
import com.linkedin.drelephant.configurations.fetcher.FetcherConfigurationData
import com.linkedin.drelephant.security.HadoopSecurity
import com.linkedin.drelephant.spark.legacydata.SparkApplicationData
import com.linkedin.drelephant.util.{HadoopUtils, SparkUtils, Utils}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.SparkConf


/**
 * A wrapper that replays Spark event history from files and then fill proper data objects.
 */
class SparkFSFetcher(fetcherConfData: FetcherConfigurationData) extends ElephantFetcher[SparkApplicationData] {
  import SparkFSFetcher._

  val eventLogSizeLimitMb =
    Option(fetcherConfData.getParamMap.get(LOG_SIZE_XML_FIELD))
      .flatMap { x => Option(Utils.getParam(x, 1)) }
      .map { _(0) }
      .getOrElse(DEFAULT_EVENT_LOG_SIZE_LIMIT_MB)
  logger.info("The event log limit of Spark application is set to " + eventLogSizeLimitMb + " MB")
  val eventLogUri = Option(fetcherConfData.getParamMap.get(LOG_LOCATION_URI_XML_FIELD))
  logger.info("The event log location of Spark application is set to " + eventLogUri)

  private lazy val security = HadoopSecurity.getInstance()

  protected lazy val hadoopUtils: HadoopUtils = HadoopUtils

  protected lazy val sparkUtils: SparkUtils = SparkUtils

  protected lazy val hadoopConfiguration: Configuration = new Configuration()

  protected lazy val sparkConf: SparkConf = {
    val sparkConf = new SparkConf()
    sparkUtils.getDefaultPropertiesFile() match {
      case Some(filename) => sparkConf.setAll(sparkUtils.getPropertiesFromFile(filename))
      case None => throw new IllegalStateException("can't find Spark conf; please set SPARK_HOME or SPARK_CONF_DIR")
    }
    sparkConf
  }

  def fetchData(analyticJob: AnalyticJob): SparkApplicationData = {
    val appId = analyticJob.getAppId()
    val applicationData = doAsPrivilegedAction { () => doFetchData(appId)
    }
    // use usp.param instead of name, tdw.username instead of user
    analyticJob.setName(applicationData.getConf().getProperty("spark.hadoop.usp.param", applicationData.getConf().getProperty("usp.param",analyticJob.getName)))
    analyticJob.setUser(applicationData.getConf().getProperty("spark.submitter", applicationData.getConf().getProperty("tdw.username", analyticJob.getName)))
    applicationData
  }

  protected def doAsPrivilegedAction[T](action: () => T): T =
    security.doAs[T](new PrivilegedAction[T] { override def run(): T = action() })

  protected def doFetchData(appId: String): SparkDataCollection = {

    val dataCollection = new SparkDataCollection()
    // using supergroup
    hadoopConfiguration.set("hadoop.job.ugi","hdfsadmin,supergroup")

    val (eventLogFileSystem, baseEventLogPath) =
      sparkUtils.fileSystemAndPathForEventLogDir(hadoopConfiguration, sparkConf, eventLogUri, appId)
    val (eventLogPath, eventLogCodec) =
      sparkUtils.pathAndCodecforEventLog(sparkConf, eventLogFileSystem, baseEventLogPath, appId, None)

    // Check if the log parser should be throttled when the file is too large.
    val shouldThrottle = eventLogFileSystem.getFileStatus(eventLogPath).getLen() > (eventLogSizeLimitMb * FileUtils.ONE_MB)
    if (shouldThrottle) {
      dataCollection.throttle()
      // Since the data set is empty, we need to set the application id,
      // so that we could detect this is Spark job type
      dataCollection.getGeneralData().setApplicationId(appId)
      dataCollection.getConf().setProperty("spark.app.id", appId)

      logger.info("The event log of Spark application: " + appId + " is over the limit size of "
        + eventLogSizeLimitMb + " MB, the parsing process gets throttled.")
    } else {
      logger.info("Replaying Spark logs for application: " + appId +
                          " withlogPath: " + eventLogPath +
                          " with codec:" + eventLogCodec)
      //if event log file corrupt ,then logging instead retry.
      try {
        if (eventLogCodec.isDefined) {
          sparkUtils.withEventLog(eventLogFileSystem, eventLogPath, eventLogCodec) { in =>
            dataCollection.load(in, eventLogPath.toString)
          }
        }
        else{
          sparkUtils.withEventLog(eventLogFileSystem, eventLogPath, None) { in =>
            dataCollection.load(in, eventLogPath.toString)
          }
        }
      }
      catch {
        case ex: MalformedInputException => logger.error("Log File corrupt for: " + appId + "location is : "+ eventLogPath.toString())
      }
      finally {
        logger.info("Replay completed for application: " + appId)
      }
    }
    dataCollection
  }
}

object SparkFSFetcher {
  private val logger = Logger.getLogger(SparkFSFetcher.getClass)

  val DEFAULT_EVENT_LOG_SIZE_LIMIT_MB = 100d; // 100MB

  val LOG_SIZE_XML_FIELD = "event_log_size_limit_in_mb"

  val LOG_LOCATION_URI_XML_FIELD = "event_log_location_uri"

  val DEFAULT_ATTEMPT_ID = Some("1")
}
