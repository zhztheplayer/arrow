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

import java.io.File;
import java.util.Collections;

import org.apache.arrow.dataset.datasource.DataSourceDiscovery;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSetDataSourceDiscovery;
import org.apache.arrow.dataset.file.FileSystem;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

public class NativeDataSourceTest {

  @Test
  public void testDataSourceDiscovery() {
    String path = NativeDataSourceTest.class.getResource(File.separator + "users.parquet").getPath();
    DataSourceDiscovery discovery = new FileSetDataSourceDiscovery(
        new RootAllocator(Long.MAX_VALUE), FileFormat.PARQUET, FileSystem.LOCAL,
        Collections.singletonList(path));
    Schema inspect = discovery.inspect();
    System.out.println(inspect);
  }
}
