/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.dataset.scanner;

import org.apache.arrow.dataset.filter.Filter;

public class ScanOptions {
  private final String[] columns;
  private final Filter filter;
  private final long batchSize;

  public ScanOptions(String[] columns, Filter filter, long batchSize) {
    this.columns = columns;
    this.filter = filter;
    this.batchSize = batchSize;
  }

  public String[] getColumns() {
    return columns;
  }

  public Filter getFilter() {
    return filter;
  }

  public long getBatchSize() {
    return batchSize;
  }
}
