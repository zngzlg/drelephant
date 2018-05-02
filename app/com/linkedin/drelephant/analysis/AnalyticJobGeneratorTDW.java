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

package com.linkedin.drelephant.analysis;

import com.linkedin.drelephant.ElephantContext;
import controllers.MetricsController;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.URL;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * This class provides a list of analysis promises to be generated under Hadoop YARN environment
 */
public class AnalyticJobGeneratorTDW implements AnalyticJobGenerator {
    private static final Logger logger = Logger.getLogger(AnalyticJobGeneratorTDW.class);
    private static final Logger trace = Logger.getLogger("TRACE");
    private static final String TDW_RESOURCEMANAGER_ADDRESS = "tdw.resourcemanager.address";
    private static final String TDW_JOB_FETCHWINTOW = "tdw.fetch.window";
    private static final String TDW_FETCH_TYPE = "tdw.fetch.type";
    private static final String TDW_FETCH_VERSION = "tdw.fetch.version";

    private static Configuration configuration;


    private String _resourceManagerAddress;
    private int _tdwFetchWindow;
    private String _tdwFetchType;
    private String _tdwFetchVersion;
    private final ObjectMapper _objectMapper = new ObjectMapper();

    private final Queue<AnalyticJob> _firstRetryQueue = new ConcurrentLinkedQueue<AnalyticJob>();

    private final ArrayList<AnalyticJob> _secondRetryQueue = new ArrayList<AnalyticJob>();

    public void updateResourceManagerAddresses() {

    }

    @Override
    public void configure(Configuration configuration)
            throws IOException {
        this.configuration = configuration;
        String tdwResourceManagerAddress = configuration.get(TDW_RESOURCEMANAGER_ADDRESS);
        if (tdwResourceManagerAddress == null) {
            logger.error("Can not find tdw ResourceManager address, tdw.resourcemanager.address must be configured at GeneralConf.xml");
        } else {
            _resourceManagerAddress = tdwResourceManagerAddress;
        }
        _tdwFetchWindow = configuration.getInt(TDW_JOB_FETCHWINTOW, 10);
        _tdwFetchType = configuration.get(TDW_FETCH_TYPE, "MapReduce");
        _tdwFetchVersion = configuration.get(TDW_FETCH_VERSION, "1");
        // updateResourceManagerAddresses();
    }

    /**
     * Fetch all the succeeded and failed applications/analytic jobs from the resource manager.
     *
     * @return
     * @throws IOException
     * @throws AuthenticationException
     */
    @Override
    public List<AnalyticJob> fetchAnalyticJobs()
            throws IOException, AuthenticationException {
        List<AnalyticJob> appList = new ArrayList<AnalyticJob>();
        URL url = new URL(String.format("%s?batch_num=%d&applicationtype=%s&version=%s", _resourceManagerAddress, _tdwFetchWindow, _tdwFetchType, _tdwFetchVersion));
        logger.info("fetch job list from: " + url.toString());
        try {
            appList.addAll(readApps(url));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return appList;
    }

    @Override
    public void addIntoRetries(AnalyticJob promise) {
        int retryQueueSize = _firstRetryQueue.size();
        if (retryQueueSize > 1000) {
            logger.warn("Retry queue size over limit, throw it away. (>1000)");
            return;
        }
        _firstRetryQueue.add(promise);
        retryQueueSize = retryQueueSize + 1;
        MetricsController.setRetryQueueSize(retryQueueSize);
        logger.info("Retry queue size is " + retryQueueSize);
    }

    @Override
    public void addIntoSecondRetryQueue(AnalyticJob promise) {
        int secondRetryQueueSize = _secondRetryQueue.size();
        if (secondRetryQueueSize > 1000) {
            logger.warn("Second retry queue size over limit, throw it away. (>1000)");
            return;
        }
        _secondRetryQueue.add(promise.setTimeToSecondRetry());
        secondRetryQueueSize = secondRetryQueueSize + 1;
        MetricsController.setSecondRetryQueueSize(secondRetryQueueSize);
        logger.info("Second Retry queue size is " + secondRetryQueueSize);
    }


    /**
     * Connect to url using token and return the JsonNode
     *
     * @param url The url to connect to
     * @return
     * @throws IOException             Unable to get the stream
     * @throws AuthenticationException Authencation problem
     */
    private JsonNode readJsonNode(URL url)
            throws IOException, AuthenticationException {
        return _objectMapper.readTree(url.openStream());
    }

    /**
     * Parse the returned json from Resource manager
     *
     * @param url The REST call
     * @return
     * @throws IOException
     * @throws AuthenticationException Problem authenticating to resource manager
     */
    private List<AnalyticJob> readApps(URL url) throws IOException, AuthenticationException {
        List<AnalyticJob> appList = new ArrayList<AnalyticJob>();
        JsonNode rootNode = readJsonNode(url);
        JsonNode apps = rootNode.path("apps").path("app");
        for (JsonNode app : apps) {
            String appId = app.get("id").getTextValue();

            // When called first time after launch, hit the DB and avoid duplicated analytic jobs that have been analyzed
            // before.
            // if (AppResult.find.byId(appId) == null) {
            String user = app.get("user").getTextValue();
            String name = app.get("name").getTextValue();
            String[] queueSplits = app.get("queue").getTextValue().split("[.]");
            String queueName = queueSplits[queueSplits.length-1];
            String trackingUrl = app.get("trackingUrl") != null ? app.get("trackingUrl").getTextValue() : null;
            long startTime = app.get("startedTime").getLongValue();
            long finishTime = app.get("finishedTime").getLongValue();
            Timestamp ts = new Timestamp(finishTime);
            DateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
            String ftime = sdf.format(ts);
            trace.info(String.format("tracing %s at %s", appId, ts));
            ApplicationType type =
                    ElephantContext.instance().getApplicationTypeForName(app.get("applicationType").getTextValue());
            // If the application type is supported
            if (type != null) {
                AnalyticJob analyticJob = new AnalyticJob();
                analyticJob.setAppId(appId).setAppType(type).setUser(user).setName(name).setQueueName(queueName)
                        .setTrackingUrl(trackingUrl).setStartTime(startTime).setFinishTime(finishTime).setFtime(ftime);
                appList.add(analyticJob);
                // }
            }
        }
        return appList;
    }
}
