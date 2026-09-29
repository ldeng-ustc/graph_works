// Native allocator boundary tests, deliberately outside timed E2 measurements.
#include "api/libxpgraph.h"
#include "graph/levelgraph.hpp"
#include <filesystem>
#include <memory>
#include <sys/prctl.h>
#include <sys/resource.h>
static void require(bool success, const char* message) {
    if (!success) throw std::runtime_error(message);
}
int main(int argc, char** argv) {
    prctl(PR_SET_DUMPABLE,0); const rlimit no_core{0,0}; setrlimit(RLIMIT_CORE,&no_core);
    if (argc != 2) return 2;
    std::filesystem::create_directories(argv[1]);
    const std::string directory = std::string(argv[1]) + '/';
    const char* config[]{"allocator-test","--ssd","1","--oneway","1","-v","1024",
        "-t","16","-s","1","-numa","0","-p0",directory.c_str(),"--membulk","4",
        "--vbuf_pool_mb","64","--pblk_pool_mb","512","--minvbuf","16","--maxvbuf","16384",
        "--elogsize","15","--batch_shift","8","--persist","0"};
    args_config(sizeof(config)/sizeof(*config),config);
    omp_set_dynamic(0);
    // Every worker requests bulks until the same shared pool is exhausted. The
    // number of successful allocations and valid returned buffers must agree.
    for (unsigned repetition = 0; repetition < 20; ++repetition) {
        auto memory = std::make_unique<char[]>(VBUF_POOL_SIZE);
        mempool_t pool; pool.init(memory.get(),VBUF_POOL_SIZE,CACHE_LINE_SIZE,"pressure");
        thd_mem_t allocator(&pool,nullptr,nullptr,nullptr);
        size_t allocated = 0;
#pragma omp parallel num_threads(THD_COUNT) reduction(+:allocated)
        {
#pragma omp barrier
            while (auto* buffer = allocator.alloc_vbuf(MAX_VBUF_SIZE)) {
                require(buffer->get_max_nebrcount() == MAX_VBUF_CAP,"bad buffer capacity");
                buffer->get_adjlist()[0] = 19;
                buffer->get_adjlist()[MAX_VBUF_CAP-1] = 23;
                allocated += MAX_VBUF_SIZE;
            }
        }
        require(allocated == VBUF_POOL_SIZE,"allocator lost or exceeded pool capacity");
        require(pool.get_remained() == 0,"expected exhausted pool");
    }
    // Force the exact-full-block fallback with a two-buffer DRAM pool. This
    // deterministically used to append the fifth edge into a full 4-edge block.
    metrics metric("allocator-test");
    levelgraph_t storage(1024,metric); storage.init_elog(); storage.init_graph();
    const size_t old_bulk = MEM_BULK_SIZE; MEM_BULK_SIZE = 32;
    char dram[32]; mempool_t tiny_pool; tiny_pool.init(dram,sizeof(dram),16,"two-buffers");
    mempool_t* persistent[]{storage.get_pblk_pool(0)};
    {
        graph_t fixture(false,4,&tiny_pool,persistent,nullptr,nullptr,metric);
        fixture.set_snap_id(0);
        edge_t first[]{{0,1},{0,1}};
        fixture.buffer_range_edges(first,2,0); fixture.archive_vbuf(0);
        edge_t occupy[]{{1,2},{1,2}}; fixture.buffer_range_edges(occupy,2,0);
        fixture.buffer_range_edges(first,2,0);
        edge_t last[]{{0,1}}; fixture.buffer_range_edges(last,1,0);
        uint32_t neighbors[5];
        require(fixture.get_degree(0)==5 && fixture.get_nebrs(0,neighbors)==5,"fallback lost edges");
        for (auto dst : neighbors) require(dst==1,"fallback changed duplicate edge");
        require(fixture.get_vertex(0)->get_1st_block()->get_next()!=nullptr,"expected second persistent block");
    }
    MEM_BULK_SIZE = old_bulk;
    storage.sync_storage(); storage.free_graph(); storage.free_elog();
    std::puts("PASS: 20 concurrent pool exhaustion rounds and exact-full-block fallback");
}
