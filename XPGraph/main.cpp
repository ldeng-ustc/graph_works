
// #include <omp.h>
// #include <getopt.h>
// #include <stdlib.h>

#include <cstdio>
#include <iostream>
#include <string>
#include <fstream>

#include "libxpgraph.h"
#include "apps/xpgraph_benchmarks.hpp"

int main(int argc, const char ** argv)
{
    XPGraph *xpgraph = new XPGraph(argc, argv);
    xpgraph->import_graph();
    
    test_graph_benchmarks_gapbs(xpgraph);
    delete xpgraph;
    return 0;
}
