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

import java.util.logging.Logger;
import javax.inject.Inject;

import com.twitter.heron.healthmgr.HealthPolicyConfig;
import com.twitter.heron.healthmgr.sensors.ExecuteCountSensor;

import static com.twitter.heron.healthmgr.common.HealthMgrConstants.SYMPTOM_PROCESSING_RATE_SKEW;

public class ProcessingRateSkewDetector extends SkewDetector {
  public static final String CONF_SKEW_RATIO = "ProcessingRateSkewDetector.skewRatio";

  private static final Logger LOG = Logger.getLogger(ProcessingRateSkewDetector.class.getName());

  @Inject
  ProcessingRateSkewDetector(ExecuteCountSensor exeCountSensor,
                             HealthPolicyConfig policyConfig) {
    super(exeCountSensor, Double.valueOf(policyConfig.getConfig(CONF_SKEW_RATIO, "1.5")),
        SYMPTOM_PROCESSING_RATE_SKEW);
  }

}