#ifndef LSMG_FILE_HEADER
#define LSMG_FILE_HEADER

#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

namespace lsmg {
class FileIO {
 public:
  FileIO(const std::string &filename)
      : fd_(-1)
      , filename_(filename) {}

  bool Open(int flags, mode_t mode = 0666) {
    fd_ = open(filename_.c_str(), flags, mode);
    return (fd_ != -1);
  }

  void Close() {
    if (fd_ != -1) {
      close(fd_);
      fd_ = -1;
    }
  }

  ssize_t Write(const void *data, size_t size) {
    if (fd_ == -1) {
      return -1;
    }
    return write(fd_, data, size);
  }

  ssize_t Read(void *data, size_t size) {
    if (fd_ == -1) {
      return -1;
    }
    return read(fd_, data, size);
  }

  bool IsOpen() const {
    return (fd_ != -1);
  }

  int GetFileDescriptor() const {
    return fd_;
  }

  std::string GetFileName() const {
    return filename_;
  }

  ~FileIO() {
    Close();
  }

 private:
  int         fd_;
  std::string filename_;
};
}  // namespace lsmg
#endif