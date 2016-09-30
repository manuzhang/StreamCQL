/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.huawei.streaming.gearpump;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.huawei.streaming.application.ApplicationResults;
import org.apache.gearpump.cluster.MasterToAppMaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class GearpumpApplicationResults implements ApplicationResults {

  private static final Logger LOG = LoggerFactory.getLogger(GearpumpApplication.class);

  private static final String FORMATTER = "%-20s %-10s";

  private static final String[] RESULTS_HEAD = {"appName", "appId"};

  private static final String ANY_CHAR_IN_CQL = "*";

  private static final String REGULAR_ANY_CHAR = ".*";

  private final List<String[]> results;

  public GearpumpApplicationResults(int appId, String appName) {
    results = Lists.newArrayList();
    String[] result = new String[RESULTS_HEAD.length];
    result[0] = appId + "";
    result[1] = appName;
    results.add(result);
  }

  @Override
  public String getFormatter() {
    return FORMATTER;
  }

  @Override
  public String[] getResultHeader() {
    return  RESULTS_HEAD;
  }

  @Override
  public List<String[]> getResults(String container) {
    List<String[]> filteredResults = Lists.newArrayList();

    if (Strings.isNullOrEmpty(container))
    {
      return results;
    }

    Pattern funcPattern;
    try
    {
      funcPattern = Pattern.compile(container.replace(ANY_CHAR_IN_CQL, REGULAR_ANY_CHAR));
    }
    catch (PatternSyntaxException e)
    {
      LOG.error("Failed to compile " + container + " to pattern", e);
      return filteredResults;
    }

    for (String[] result : results)
    {
      String name = result[0];
      if (funcPattern.matcher(name).matches())
      {
        filteredResults.add(result);
      }

    }

    return filteredResults;
  }
}
