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

package org.apache.arrow.dataset.parquet;

import org.apache.arrow.dataset.FileSource;
import org.apache.arrow.dataset.ScanOptions;

import java.util.Iterator;

public class ParquetScanTaskIterable implements Iterable<ParquetScanTask> {
  private final FileSource source;
  private final ScanOptions options;

  public ParquetScanTaskIterable(FileSource source, ScanOptions options) {
    this.source = source;
    this.options = options;
  }

  @Override
  public Iterator<ParquetScanTask> iterator() {
    return new Itr();
  }

  private class Itr implements Iterator<ParquetScanTask> {
    @Override
    public boolean hasNext() {
      return false; // todo jni
    }

    @Override
    public ParquetScanTask next() {
      return null;
    }
  }
}
