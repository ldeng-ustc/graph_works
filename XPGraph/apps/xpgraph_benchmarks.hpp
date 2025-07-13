#pragma once
#include <iostream>
#include <string>
#include <fstream>
#include <sys/time.h>

#include <libxpgraph.h>

#include "breadth_first_search.hpp"
#include "pr_gapbs.hpp"
#include "connected_components.hpp"

#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>
long get_rss() {
    std::ifstream stat_file("/proc/self/stat");
    std::string line;
    std::getline(stat_file, line);
    std::istringstream iss(line);
    std::string token;

    // 跳过前 23 个字段
    for (int i = 0; i < 23; ++i) {
        iss >> token;
    }

    // 读取第 24 个字段（RSS）
    iss >> token;
    return std::stol(token) * sysconf(_SC_PAGESIZE);
}

inline double mywtime()
{
	double time[2];	
	struct timeval time1;
	gettimeofday(&time1, NULL);

	time[0]=time1.tv_sec;
	time[1]=time1.tv_usec;

	return time[0] + time[1]*1.0e-6;
}

#ifndef EXPOUT
#define EXPOUT "[EXPOUT]"
#endif

void test_graph_benchmarks_gapbs(XPGraph* xpgraph){
    long rss_ingest = get_rss();

    auto st = mywtime();
    for(size_t i=0; i<20; i++)
        test_bfs_numa(xpgraph, i);
    auto ts1 = mywtime();

    long rss_bfs = get_rss();

    auto res = pr_gapbs(xpgraph, 10);
    auto ts2 = mywtime();
    delete [] res;

    auto ts3_st = mywtime();
    for(size_t i=0; i<10; i++)
        test_connected_components_numa(xpgraph, 2);
    auto ts3 = mywtime();
    
    auto t_bfs = ts1 - st;
    auto t_pr = ts2 - ts1;
    auto t_cc = ts3 - ts3_st;

    printf(EXPOUT "BFS: %.3fs\n", t_bfs);
    printf(EXPOUT "PR: %.3fs\n", t_pr);
    printf(EXPOUT "CC: %.3fs\n", t_cc);

    printf(EXPOUT "RSS_Ingest: %ld\n", rss_ingest);
    printf(EXPOUT "RSS_BFS: %ld\n", rss_bfs);
}
