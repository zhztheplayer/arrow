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

package org.apache.arrow.dataset.jni;

public class JniWrapper {

  private static final JniWrapper INSTANCE = new JniWrapper();

  public static JniWrapper get() {
    return INSTANCE;
  }

  private JniWrapper() {}

  public native void closeDataSource(long dataSourceId);

  public native long[] getFragments(long dataSourceId, String[] columns, byte[] filter);

  public native void closeFragment(long fragmentId);

  public native long[] getScanTasks(long fragmentId);

  public native void closeScanTask(long scanTaskId);

  public native long scan(long scanTaskId);

  public native void closeIterator(long id);

  public native NativeRecordBatchHandle nextRecordBatch(long recordBatchIteratorId);

  public native void releaseBuffer(long bufferId);

}
