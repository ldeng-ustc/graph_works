#pragma once
#include <chrono>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/stat.h>

namespace e12_livegraph {
using Clock = std::chrono::steady_clock;
inline double seconds(Clock::time_point start) {
    return std::chrono::duration<double>(Clock::now() - start).count();
}
inline void disable_cores() {
    const rlimit limit{0, 0};
    ::prctl(PR_SET_DUMPABLE, 0, 0, 0, 0);
    ::setrlimit(RLIMIT_CORE, &limit);
}
inline void create_runtime(const std::filesystem::path& path) {
    if (std::filesystem::exists(path)) throw std::runtime_error("runtime already exists; choose a new path");
    if (!std::filesystem::create_directories(path)) throw std::runtime_error("cannot create runtime");
}
class Json {
public:
    explicit Json(std::string_view kind) {
        out_ << std::setprecision(17) << '{';
        text("kind", kind).number("schema", 1).text("system", "livegraph");
    }
    Json& text(std::string_view key, std::string_view value) {
        field_(key); quote_(value); return *this;
    }
    template<class T> Json& number(std::string_view key, T value) {
        if constexpr (std::is_floating_point_v<T>)
            if (!std::isfinite(value)) throw std::runtime_error("nonfinite metric");
        field_(key); out_ << value; return *this;
    }
    void emit() {
        const std::string line = "E12_JSON " + out_.str() + "}\n";
        std::fwrite(line.data(), 1, line.size(), stdout);
        std::fflush(stdout);
    }
private:
    void field_(std::string_view key) {
        if (!first_) out_ << ',';
        first_ = false;
        quote_(key); out_ << ':';
    }
    void quote_(std::string_view value) {
        constexpr char hex[] = "0123456789abcdef";
        out_ << '"';
        for (unsigned char c : value) {
            if (c == '\\' || c == '"') out_ << '\\' << c;
            else if (c < 32) out_ << "\\u00" << hex[c >> 4] << hex[c & 15];
            else out_ << c;
        }
        out_ << '"';
    }
    bool first_ = true;
    std::ostringstream out_;
};
struct Footprint { uint64_t physical = 0, logical = 0; };
inline Footprint footprint(const std::filesystem::path& root) {
    Footprint result;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) continue;
        struct stat value{};
        if (::stat(entry.path().c_str(), &value) != 0) throw std::runtime_error("stat failed");
        result.physical += static_cast<uint64_t>(value.st_blocks) * 512;
        result.logical += static_cast<uint64_t>(value.st_size);
    }
    return result;
}
}  // namespace e12_livegraph

