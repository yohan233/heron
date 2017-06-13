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

package com.twitter.heron.healthmgr.detectors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.metrics.ComponentMetrics;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.sensors.BackPressureSensor;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.METRIC_BACK_PRESSURE;
import static com.twitter.heron.healthmgr.detectors.BackPressureDetector.CONF_NOISE_FILTER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BackPressureDetectorTest {
  @Test
  public void testConfigAndFilter() {
    HealthPolicyConfig config = mock(HealthPolicyConfig.class);
    when(config.getConfig(CONF_NOISE_FILTER, "20")).thenReturn("50");

    ComponentMetrics compMetrics =
        new ComponentMetrics("bolt", "i1", METRIC_BACK_PRESSURE, 55);
    Map<String, ComponentMetrics> topologyMetrics = new HashMap<>();
    topologyMetrics.put("bolt", compMetrics);

    BackPressureSensor sensor = mock(BackPressureSensor.class);
    when(sensor.get()).thenReturn(topologyMetrics);

    BackPressureDetector detector = new BackPressureDetector(sensor, config);
    List<Symptom> symptoms = detector.detect();

    Assert.assertEquals(1, symptoms.size());

    compMetrics = new ComponentMetrics("bolt", "i1", METRIC_BACK_PRESSURE, 45);
    topologyMetrics.put("bolt", compMetrics);

    sensor = mock(BackPressureSensor.class);
    when(sensor.get()).thenReturn(topologyMetrics);

    detector = new BackPressureDetector(sensor, config);
    symptoms = detector.detect();

    Assert.assertEquals(0, symptoms.size());
  }
}
