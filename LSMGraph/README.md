[LSMGraph](https://dl.acm.org/doi/10.1145/3698818) is a dynamic graph storage system that combines the write-friendly LSM-tree with the read-efficient CSR representation. 

LSMGraph leverages the multi-level structure of LSM-trees to optimize write performance while utilizing the compact CSR structures embedded in the LSM-trees to boost read performance. LSMGraph uses a new in-memory structure, MemGraph, to efficiently cache graph updates and uses a multi-level index to speed up reads within the multi-level structure. Furthermore, LSMGraph incorporates a vertex-grained version control mechanism to mitigate the impact of LSM-tree compaction on read performance and ensure the correctness of concurrent read and write operations.

The overall architecture of LSMGraph is shown below:
 <p align="center">
 <img src="./LSMGraph_Architecture.png" alt="architecture" width="400"/>
 </p>


## Dependencies
- [CMake](https://gitlab.kitware.com/cmake/cmake)
- [gflags](https://github.com/gflags/gflags.git)
- OpenMP
- GCC(11.4.0)
- [TBB](https://github.com/oneapi-src/oneTBB) 
- cgroups
- [fmt(10.2.0)](https://github.com/fmtlib/fmt/tree/master)
- [spdlog](https://github.com/gabime/spdlog)

Here are the dependencies for optional features:
- tcmalloc
- clang-format

## Quick Start
```shell
mkdir build && cd build
cmake ..
make -j8 bench_run_algo
../run.sh
```

## **References**
Please cite LSMGraph in your publications if it helps your research:
```
@article{DBLP:journals/pacmmod/YuGTSZYLZLLYZ24,
  author       = {Song Yu and
                  Shufeng Gong and
                  Qian Tao and
                  Sijie Shen and
                  Yanfeng Zhang and
                  Wenyuan Yu and
                  Pengxi Liu and
                  Zhixin Zhang and
                  Hongfu Li and
                  Xiaojian Luo and
                  Ge Yu and
                  Jingren Zhou},
  title        = {LSMGraph: {A} High-Performance Dynamic Graph Storage System with Multi-Level
                  {CSR}},
  journal      = {Proc. {ACM} Manag. Data},
  volume       = {2},
  number       = {6},
  pages        = {243:1--243:28},
  year         = {2024}
}
```