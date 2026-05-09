// This code is part of the project "Ligra: A Lightweight Graph Processing
// Framework for Shared Memory", presented at Principles and Practice of 
// Parallel Programming, 2013.
// Copyright (c) 2013 Julian Shun and Guy Blelloch
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights (to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
// #include "ligra.h"
#include "math.h"
#pragma once
#include "Map.cpp"
#define newA(__E,__n) (__E*) malloc((__n)*sizeof(__E))


// Credits to :
// http://www.memoryhole.net/kyle/2012/06/a_use_for_volatile_in_multithr.html
typedef float rank_t; 
float qthread_dincr(float *operand, float incr)
{
    //*operand = *operand + incr;
    //return incr;
    
    union {
       rank_t   d;
       uint32_t i;
    } oldval, newval, retval;
    do {
         oldval.d = *(volatile rank_t *)operand;
         newval.d = oldval.d + incr;
         //__asm__ __volatile__ ("lock; cmpxchgq %1, (%2)"
         __asm__ __volatile__ ("lock; cmpxchg %1, (%2)"
                                : "=a" (retval.i)
                                : "r" (newval.i), "r" (operand),
                                 "0" (oldval.i)
                                : "memory");
    } while (retval.i != oldval.i);
    return oldval.d;
}


// template <class vertex>
template<typename T, class Graph>
struct PR_Push_F {
  T* p_curr, *p_next;

  Graph& G;
  PR_Push_F(T* _p_curr, T* _p_next, Graph& _G) : 
    p_curr(_p_curr), p_next(_p_next), G(_G) {}
  inline bool update(uint32_t s, uint32_t d){ //update function applies PageRank equation
    (void) s;
    (void) d;
    printf("unexpected use of update. (push style is always sparse.)\n");
    exit(1);
    return 1;
  }

  inline bool updateAtomic (uint32_t s, uint32_t d) { //atomic Update
    qthread_dincr(&p_next[d], p_curr[s]/G.degree(s));
    return 1;
  }

  inline bool cond ([[maybe_unused]] uint32_t  d) { return 1; }

};


template<typename T, class Graph>
struct PR_Push_Vertex {
  T *p_next;
  T *dset;
  T *p_curr;
  Graph& G;

  PR_Push_Vertex(T* _p_next, T* _dset, T* _p_curr, Graph& _G) : p_next(_p_next), dset(_dset), p_curr(_p_curr), G(_G) {}

  inline bool operator () (uint32_t i) {
    p_next[i] = (0.15 + 0.85 * p_next[i]) * dset[i];
    p_curr[i] = 0.0;
    return 1;
  }
};

template<typename T, class Graph>
T* PR_Push_S(Graph& G, long maxIters) {
  size_t n = G.get_num_vertices();
  T* p_curr = (T*) memalign(32, n*sizeof(T));
  T* p_next = (T*) memalign(32, n*sizeof(T));
  T* dset = (T*) memalign(32, n*sizeof(T));

  const T inv_v_count = 0.15;
  parallel_for(size_t v = 0; v < n; v++) {
    size_t degree = G.degree(v);
    if (degree != 0) {
        dset[v] = 1.0f / degree;
        p_curr[v] = inv_v_count;
    } else {
        dset[v] = 0;
        p_curr[v] = 0;
    }
  }
  VertexSubset Frontier = VertexSubset(0, n, true); // all vertex
  
  long iter = 0;
  printf("max iters %lu\n", maxIters);
  while(iter++ < maxIters) {
    edgeMap(G, Frontier, PR_Push_F(p_curr, p_next, G), false, 0);
    vertexMap(Frontier,PR_Push_Vertex(p_next, dset, p_curr, G), false);
    // vertexMap(Frontier,PR_Push_Vertex_Reset(p_next), false);
    std::swap(p_curr,p_next);
  }

  free(p_next);
  return p_curr;
}
