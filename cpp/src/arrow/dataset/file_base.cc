// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/dataset/file_base.h"

#include <algorithm>
#include <memory>
#include <vector>

#include "arrow/dataset/filter.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/util/iterator.h"

namespace arrow {
namespace dataset {

Result<std::shared_ptr<arrow::io::RandomAccessFile>> FileSource::Open() const {
  switch (type_) {
    case PATH:
      return filesystem_->OpenInputFile(path_);
    case BUFFER:
      return std::make_shared<::arrow::io::BufferReader>(buffer_);
  }

  return Status::NotImplemented("Unknown file source type.");
}

Result<ScanTaskIterator> FileDataFragment::Scan(ScanContextPtr context) {
  return format_->ScanFile(source_, scan_options_, context);
}

FileSystemDataSource::FileSystemDataSource(fs::FileSystemPtr filesystem,
                                           fs::PathForest forest,
                                           ExpressionPtr source_partition,
                                           PathPartitions partitions,
                                           FileFormatPtr format)
    : DataSource(std::move(source_partition)),
      filesystem_(std::move(filesystem)),
      forest_(std::move(forest)),
      partitions_(std::move(partitions)),
      format_(std::move(format)) {}

Result<DataSourcePtr> FileSystemDataSource::Make(fs::FileSystemPtr filesystem,
                                                 fs::FileStatsVector stats,
                                                 ExpressionPtr source_partition,
                                                 PathPartitions partitions,
                                                 FileFormatPtr format) {
  fs::PathForest forest;
  RETURN_NOT_OK(fs::PathTree::Make(stats, &forest));

  return DataSourcePtr(new FileSystemDataSource(
      std::move(filesystem), std::move(forest), std::move(source_partition),
      std::move(partitions), std::move(format)));
}

DataFragmentIterator FileSystemDataSource::GetFragmentsImpl(ScanOptionsPtr options) {
  std::vector<std::unique_ptr<fs::FileStats>> files;

  auto visitor = [&files](const fs::FileStats& stats) {
    if (stats.IsFile()) {
      files.emplace_back(new fs::FileStats(stats));
    }
    return Status::OK();
  };
  // The matcher ensures that directories (and their descendants) are not
  // visited.
  auto matcher = [this, options](const fs::FileStats& stats, bool* match) {
    *match = this->PartitionMatches(stats, options->filter);
    return Status::OK();
  };

  for (auto tree : forest_) {
    DCHECK_OK(tree->Visit(visitor, matcher));
  }

  auto file_it = MakeVectorIterator(std::move(files));
  auto file_to_fragment = [options, this](std::unique_ptr<fs::FileStats> stats,
                                          std::shared_ptr<DataFragment>* out) {
    FileSource src(stats->path(), filesystem_.get());
    ARROW_ASSIGN_OR_RAISE(*out, format_->MakeFragment(src, options));
    return Status::OK();
  };

  return MakeMaybeMapIterator(file_to_fragment, std::move(file_it));
}

bool FileSystemDataSource::PartitionMatches(const fs::FileStats& stats,
                                            ExpressionPtr filter) {
  if (filter == nullptr) {
    return true;
  }

  auto found = partitions_.find(stats.path());
  if (found == partitions_.end()) {
    // No partition attached to current node (directory or file), continue.
    return true;
  }

  auto expr = found->second->Assume(*filter);
  if (expr->IsNull() || expr->Equals(false)) {
    // selector is not satisfiable; don't recurse in this branch.
    return false;
  }

  return true;
}

FileSetDataSource::FileSetDataSource(FileSourceVector files, FileFormatPtr format)
    : files_(std::move(files)),
      format_(std::move(format)) {}

DataFragmentIterator FileSetDataSource::GetFragmentsImpl(ScanOptionsPtr options) {
  auto file_srcs_it = MakeVectorIterator(std::move(files_));
  auto file_src_to_fragment = [options, this](const FileSourcePtr& source, std::shared_ptr<DataFragment>* out) {
    ARROW_ASSIGN_OR_RAISE(*out, format_->MakeFragment(*source, options));
    return Status::OK();
  };
  return MakeMaybeMapIterator(file_src_to_fragment, std::move(file_srcs_it));
}

Result<DataSourcePtr> FileSetDataSource::Make(const std::vector<std::string>& paths,
                                              const fs::FileSystemPtr &fs,
                                              FileFormatPtr format) {
  FileSourceVector file_srcs;
  for (const auto& path : paths) {
    std::shared_ptr<FileSource> file_src = std::make_shared<FileSource>(path, fs.get());
    file_srcs.push_back(std::move(file_src));
  }
  return DataSourcePtr(
      new FileSetDataSource(file_srcs, std::move(format)));
}

Result<DataSourcePtr> FileSetDataSource::Make(FileSourceVector files, FileFormatPtr format) {
  return DataSourcePtr(new FileSetDataSource(std::move(files), std::move(format)));
}

// todo don't include following code in release version
Result<std::shared_ptr<FileSetScannerBuilder>> FileSetScannerBuilder::Make(std::vector<std::string> paths,
                                                                           FileFormatPtr file_format,
                                                                           fs::FileSystemPtr fs,
                                                                           ScanContextPtr context) {

  FileSourceVector file_src;
  for (const std::string& path : paths) {
    file_src.push_back(std::make_shared<FileSource>(path, fs.get()));
  }
  ARROW_ASSIGN_OR_RAISE(DataSourcePtr src, FileSetDataSource::Make(file_src, file_format))
  std::shared_ptr<Schema> schema;
  if (file_src.empty()) {
    schema = std::shared_ptr<Schema>(nullptr); // fixme merge schemas
  } else {
    ARROW_ASSIGN_OR_RAISE(schema, file_format->Inspect(*file_src.at(0)));
  }
  ARROW_ASSIGN_OR_RAISE(DatasetPtr dataset, Dataset::Make({src}, schema));
  return std::make_shared<FileSetScannerBuilder>(dataset, std::move(context));
}

}  // namespace dataset
}  // namespace arrow
