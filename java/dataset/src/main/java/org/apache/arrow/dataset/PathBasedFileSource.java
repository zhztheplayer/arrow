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

import org.apache.arrow.compression.CompressionType;
import org.apache.arrow.fs.FileSystem;

public class PathBasedFileSource implements FileSource {

  private final String path;
  private final FileSystem fileSystem;
  private final CompressionType compression;

  public PathBasedFileSource(String path, FileSystem fileSystem, CompressionType compression) {
    this.path = path;
    this.fileSystem = fileSystem;
    this.compression = compression;
  }

  public CompressionType compression() {
    return compression;
  }

  public String path() {
    return path;
  }

  public FileSystem fileSystem() {
    return fileSystem;
  }
}
