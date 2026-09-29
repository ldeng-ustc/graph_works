// Small mmap read-concurrency probe; independent of graph implementation and not a benchmark result.
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <numeric>
#include <random>
#include <stdexcept>
#include <vector>
#include <omp.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <sys/prctl.h>
#include <unistd.h>
int main(int argc, char** argv) {
    prctl(PR_SET_DUMPABLE, 0); const rlimit no_core{0,0}; setrlimit(RLIMIT_CORE,&no_core);
    if (argc != 2) return 2;
    std::filesystem::create_directories(argv[1]);
    const auto path = std::filesystem::path(argv[1]) / "mmap-probe.dat";
    const int fd = open(path.c_str(),O_RDWR|O_CREAT|O_EXCL,0600);
    if (fd < 0) return 2;
    constexpr size_t bytes = size_t(128) << 20;
    const size_t page = sysconf(_SC_PAGESIZE), pages = bytes / page;
    if (ftruncate(fd,bytes)) return 2;
    auto* data = static_cast<uint64_t*>(mmap(nullptr,bytes,PROT_READ|PROT_WRITE,MAP_SHARED,fd,0));
    if (data == MAP_FAILED) return 2;
    for (size_t i = 0; i < bytes/sizeof(*data); ++i) data[i] = i * 11939 + 71;
    if (msync(data,bytes,MS_SYNC) || fdatasync(fd)) return 2;
    std::vector<size_t> indices(pages); std::iota(indices.begin(),indices.end(),0);
    std::vector<unsigned char> residency(pages);
    std::puts("threads,pattern,cold_resident_pages,pages,seconds,mib_per_second,major_faults,checksum");
    for (unsigned repetition = 0; repetition < 2; ++repetition) {
        for (unsigned threads : {1u,2u,4u,8u,16u}) {
            for (const bool random : {false,true}) {
                std::iota(indices.begin(),indices.end(),0);
                if (random) { std::mt19937 generator(20260930 + repetition); std::shuffle(indices.begin(),indices.end(),generator); }
                if (madvise(data,bytes,MADV_DONTNEED) || posix_fadvise(fd,0,bytes,POSIX_FADV_DONTNEED)) return 2;
                if (mincore(data,bytes,residency.data())) return 2;
                size_t resident = 0; for (auto status : residency) resident += status & 1;
                madvise(data,bytes,random ? MADV_RANDOM : MADV_SEQUENTIAL);
                rusage before{},after{}; getrusage(RUSAGE_SELF,&before);
                uint64_t checksum = 0; const auto begin = std::chrono::steady_clock::now();
#pragma omp parallel for num_threads(threads) schedule(static) reduction(+:checksum)
                for (size_t i = 0; i < pages; ++i) checksum += static_cast<volatile uint64_t*>(data)[indices[i]*page/sizeof(*data)];
                const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now()-begin).count();
                getrusage(RUSAGE_SELF,&after);
                std::printf("%u,%s,%zu,%zu,%.9f,%.3f,%ld,%lu\n",threads,random?"random":"sequential",resident,pages,seconds,128/seconds,after.ru_majflt-before.ru_majflt,checksum);
                std::fflush(stdout);
            }
        }
    }
    munmap(data,bytes); close(fd);
}
