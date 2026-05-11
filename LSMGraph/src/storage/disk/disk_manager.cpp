#include "storage/disk/disk_manager.h"
#include <fmt/core.h>
#include <sys/stat.h>
#include <cassert>
#include <cstring>
#include <exception>
#include <iostream>
#include <mutex>
#include <string>
#include "common/utils/logger.h"

namespace lsmg {

static char *buffer_used;

DiskManager::DiskManager(const std::string &db_file)
    : file_name_(db_file) {
  std::string::size_type n = file_name_.rfind('.');
  if (n == std::string::npos) {
    LOG_DEBUG("wrong file format");
    return;
  }

  std::scoped_lock scoped_db_io_latch(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
    if (!db_io_.is_open()) {
      std::terminate();
    }
  }
  buffer_used = nullptr;
}

/**
 * Close all file streams
 */
void DiskManager::ShutDown() {
  {
    std::scoped_lock scoped_db_io_latch(db_io_latch_);
    db_io_.close();
  }
}

void DiskManager::WritePage(page_id_t page_id, const char *page_data) {
  std::scoped_lock scoped_db_io_latch(db_io_latch_);
  size_t           offset = static_cast<size_t>(page_id) * PAGE_SIZE;
  // set write cursor to offset
  num_writes_ += 1;
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  LOG_DEBUG("write page, id: %d", page_id);
  // check for I/O error
  if (db_io_.bad()) {
    LOG_DEBUG("I/O error while writing");
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}

void DiskManager::ReadPage(page_id_t page_id, char *page_data) {
  std::scoped_lock scoped_db_io_latch(db_io_latch_);
  int              offset = page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset > GetFileSize(file_name_)) {
    LOG_DEBUG("I/O error reading past end of file");
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    if (db_io_.bad()) {
      LOG_DEBUG("I/O error while reading");
      return;
    }
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
      LOG_DEBUG("Read less than a page");
      db_io_.clear();
      // std::cerr << "Read less than a page" << std::endl;
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

int DiskManager::GetNumFlushes() const {
  return num_flushes_;
}

int DiskManager::GetNumWrites() const {
  return num_writes_;
}

bool DiskManager::GetFlushState() const {
  return flush_log_;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int         rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? static_cast<int>(stat_buf.st_size) : -1;
}

}  // namespace lsmg
