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

package org.apache.arrow.dataset;

import org.apache.arrow.filter.Filter;
import org.apache.arrow.projector.Projector;

public class ScanOptions {
  private final Filter filter;
  private final Projector projector;

  public ScanOptions(Filter filter, Projector projector) { // todo may need a builder as filter could be complex
    this.filter = filter;
    this.projector = projector;
  }
  public Filter getFilter() {
    return filter;
  }

  public Projector getProjector() {
    return projector;
  }
}
