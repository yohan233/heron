// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.twitter.heron.metricscachemgr.metricscache;

import java.io.FileNotFoundException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.metricsmgr.MetricsSinksConfig;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricDatum;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricRequest;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse;
import com.twitter.heron.proto.tmaster.TopologyMaster.PublishMetrics;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter.MetricAggregationType;

public class MetricsCache {
  public static final String METRICS_SINKS_TMASTER_SINK = "tmaster-sink";
  public static final String METRICS_SINKS_TMASTER_METRICS = "tmaster-metrics-type";
  private static final Logger LOG = Logger.getLogger(MetricsCache.class.getName());
  // map of component name to its metrics
  private Map<String, ComponentMetrics> metricsComponent;
  private int maxInterval;
  private int nintervals;
  private int interval;
  //  private int startTime;
  private MetricsFilter metricsfilter = null;

  @SuppressWarnings("unchecked")
  public MetricsCache(int maxInterval, int interval, MetricsSinksConfig sinksConfig)
      throws FileNotFoundException {
    this.maxInterval = maxInterval;
    this.interval = interval;
//    startTime = (int) Instant.now().getEpochSecond();

    metricsfilter = new MetricsFilter();
    Map<String, Object> sinksTmaster = sinksConfig.getConfigForSink(METRICS_SINKS_TMASTER_SINK);
    Map<String, String> metricsType =
        (Map<String, String>) sinksTmaster.get(METRICS_SINKS_TMASTER_METRICS);
    for (Map.Entry<String, String> e : metricsType.entrySet()) {
      metricsfilter.setMetricToType(e.getKey(), TranslateFromString(e.getValue()));
    }

    nintervals = maxInterval / interval;

    metricsComponent = new HashMap<>();
  }

  private MetricAggregationType TranslateFromString(String type) {
    if ("SUM".equals(type)) {
      return MetricAggregationType.SUM;
    } else if ("AVG".equals(type)) {
      return MetricAggregationType.AVG;
    } else if ("LAST".equals(type)) {
      return MetricAggregationType.LAST;
    } else {
      LOG.log(Level.SEVERE, "Unknown metrics type in metrics sinks " + type);
      return MetricAggregationType.UNKNOWN;
    }
  }

  public void AddMetric(PublishMetrics metrics) {
    LOG.info("AddMetric count " + metrics.getMetricsCount());
    for (int i = 0; i < metrics.getMetricsCount(); ++i) {
      String componentName = metrics.getMetrics(i).getComponentName();
      LOG.info("AddMetric componentName " + componentName);
      AddMetricsForComponent(componentName, metrics.getMetrics(i));
    }
  }

  void AddMetricsForComponent(String componentName, MetricDatum metricsData) {
    ComponentMetrics componentmetrics = GetOrCreateComponentMetrics(componentName);
    String name = metricsData.getName();
    MetricAggregationType type = metricsfilter.getAggregationType(name);
    LOG.info("AddMetricsForComponent name " + name + "; type " + type);
    componentmetrics.AddMetricForInstance(metricsData.getInstanceId(), name, type,
        metricsData.getValue());
  }

  ComponentMetrics GetOrCreateComponentMetrics(String componentName) {
    if (!metricsComponent.containsKey(componentName)) {
      metricsComponent.put(componentName,
          new ComponentMetrics(componentName, nintervals, interval));
    }
    return metricsComponent.get(componentName);
  }

  // Returns a new response to fetch metrics. The request gets propagated to Component's and
  // Instance's get metrics. Doesn't own Response.
  // (huijun) this function is deprecated
  public MetricResponse GetMetrics(MetricRequest request) {
    MetricResponse.Builder responseBuilder = MetricResponse.newBuilder();

    if (!metricsComponent.containsKey(request.getComponentName())) {
      LOG.log(Level.WARNING,
          "Metrics for component `" + request.getComponentName() + "` are not available");
      responseBuilder.setStatus(responseBuilder.getStatusBuilder()
          .setStatus(Common.StatusCode.NOTOK)
          .setMessage("Metrics not available for component `" + request.getComponentName() + "`")
          .build());
    } else if (!request.hasInterval() && !request.hasExplicitInterval()) {
      LOG.log(Level.SEVERE,
          "GetMetrics request does not have either interval" + " nor explicit interval");
      responseBuilder.setStatus(responseBuilder.getStatusBuilder()
          .setStatus(Common.StatusCode.NOTOK)
          .setMessage("No interval or explicit interval set")
          .build());
    } else {
      long startTime;
      long endTime;
      if (request.hasInterval()) {
        endTime = Instant.now().getEpochSecond();
        if (request.getInterval() <= 0) {
          startTime = 0;
        } else {
          startTime = endTime - request.getInterval();
        }
      } else {
        startTime = request.getExplicitInterval().getStart();
        endTime = request.getExplicitInterval().getEnd();
      }
      LOG.info("GetMetrics startTime: " + startTime + "; endTime: " + endTime);
      metricsComponent.get(request.getComponentName())
          .GetMetrics(request, startTime, endTime, responseBuilder);
      responseBuilder.setInterval(endTime - startTime);
    }

    return responseBuilder.build();
  }

  public void Purge() {
    for (ComponentMetrics cm : metricsComponent.values()) {
      cm.Purge();
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String k : metricsComponent.keySet()) {
      sb.append("\n").append(k).append(" #> ").append(metricsComponent.get(k).toString());
    }
    return sb.toString();
  }

  /**
   * main query interface for query from OUTside SLA process
   *
   * @param request query in protobuf format, re-used TMaster definition
   * @return query result, re-used TMaster definition
   */
  public MetricResponse GetMetrics2(MetricRequest request) {
    if (!request.hasInterval() && !request.hasExplicitInterval()) {
      MetricResponse.Builder responseBuilder = MetricResponse.newBuilder();
      LOG.log(Level.SEVERE,
          "GetMetrics request does not have either interval" + " nor explicit interval");
      responseBuilder.setStatus(responseBuilder.getStatusBuilder()
          .setStatus(Common.StatusCode.NOTOK)
          .setMessage("No interval or explicit interval set")
          .build());
      return responseBuilder.build();
    } else {
      MetricsCacheQueryUtils.MetricCacheResponse response =
          GetMetrics(MetricsCacheQueryUtils.Convert(request));
      return MetricsCacheQueryUtils.Convert(response);
    }
  }

  /**
   * main query interface for query from INside SLA process
   *
   * @param request query
   * @return query result
   */
  public MetricsCacheQueryUtils.MetricCacheResponse GetMetrics(
      MetricsCacheQueryUtils.MetricCacheRequest request) {
    MetricsCacheQueryUtils.MetricCacheResponse response =
        new MetricsCacheQueryUtils.MetricCacheResponse();
    if (!metricsComponent.containsKey(request.componentName)) {
      LOG.log(Level.WARNING,
          "Metrics for component `" + request.componentName + "` are not available");
      response.status.status = 2;
      response.status.message =
          "Metrics not available for component `" + request.componentName + "`";
    } else {
      LOG.info("query start " + request.interval.start + " end " + request.interval.end);
      metricsComponent.get(request.componentName)
          .GetMetrics(request, request.interval.start, request.interval.end, response);
    }
    return response;
  }

}
