#pragma once
#include <atomic>
#include <memory>
#include <fstream>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>

namespace e2 {
using ::algorithm::per_processor_data;
struct cpu : per_processor_data {
  uint64_t count = 0;
  double sum = 0;
  bool reduce(per_processor_data**, unsigned long) { return false; }
};
static uint64_t n, root;
static float base;
static uint32_t* degrees;
static float* input_scores;
static unsigned char* output;
static size_t output_bytes;
static std::unique_ptr<std::atomic<uint32_t>[]> parents;
static std::vector<cpu*> cpus;
inline void* mapped(const char* name, size_t bytes, bool write) {
  int fd = open(name, write ? O_RDWR|O_CREAT|O_TRUNC : O_RDONLY, 0600);
  if (fd < 0 || (write && ftruncate(fd, bytes))) { perror(name); exit(2); }
  void* p = mmap(nullptr, bytes, write ? PROT_READ|PROT_WRITE : PROT_READ, MAP_SHARED, fd, 0);
  close(fd);
  if (p == MAP_FAILED) { perror("mmap"); exit(2); }
  return p;
}
inline uint32_t find(uint32_t v) {
  uint32_t p;
  while ((p=parents[v].load(std::memory_order_relaxed)) != v) v=p;
  return v;
}
inline void unite(uint32_t a, uint32_t b) {
  while (true) {
    a=find(a); b=find(b); if (a==b) return;
    if (a<b) std::swap(a,b);
    uint32_t expected=a;
    if (parents[a].compare_exchange_weak(expected,b,std::memory_order_relaxed)) return;
  }
}
// A fixed state size makes graph partitioning identical for index/BFS/PR/CC.
template<int Mode> struct algorithm {
  struct vertex { uint32_t parent, phase; float rank; uint32_t changed; };
  struct update { uint32_t target; union { uint32_t source; float delta; }; };
  static unsigned long vertex_state_bytes() { return sizeof(vertex); }
  static unsigned long split_size_bytes() { return sizeof(update); }
  static unsigned long split_key(unsigned char* p, unsigned long jump) {
    return reinterpret_cast<update*>(p)->target >> jump;
  }
  static per_processor_data* create_per_processor_data(unsigned long) {
    cpu* p = new cpu; cpus.push_back(p); return p;
  }
  static void preprocessing() {
    n=pt.get<unsigned long>("graph.vertices");
    root=vm["bfs::root"].as<unsigned long>();
    if (Mode==2) {
      degrees=static_cast<uint32_t*>(mapped("degrees",n*4,false));
      const char* path=getenv("MBF_PR_INPUT");
      input_scores=path ? static_cast<float*>(mapped(path,n*4,false)) : nullptr;
      float initial=1.0f/static_cast<float>(n);
      double dangling=0;
      for(uint64_t v=0;v<n;++v) if(!degrees[v]) dangling+=input_scores?input_scores[v]:initial;
      base=(1.0f-0.85f+0.85f*static_cast<float>(dangling))/static_cast<float>(n);
    }
    if (Mode==3) {
      parents.reset(new std::atomic<uint32_t>[n]);
      for(uint64_t v=0;v<n;++v) parents[v].store(v,std::memory_order_relaxed);
    }
    const char* dump=getenv("MBF_E2_DUMP");
    if(dump && Mode) {
      output_bytes=n*(Mode==1?1:4);
      output=static_cast<unsigned char*>(mapped(dump,output_bytes,true));
    }
  }
  static bool init(unsigned char* p,unsigned long id,unsigned long,per_processor_data*) {
    vertex* v=reinterpret_cast<vertex*>(p);
    v->parent=(id==root ? root : UINT32_MAX); v->phase=0;
    v->rank=base; v->changed=1;
    return Mode!=1 || id==root;
  }
  static bool need_init(unsigned long phase) { return phase==0; }
  static bool check_reset_changed(unsigned char* p,unsigned long,unsigned long) {
    vertex* v=reinterpret_cast<vertex*>(p); bool changed=v->changed;
    v->changed=0; return changed;
  }
  static bool check_bsp(unsigned char* p,unsigned long,unsigned long phase,per_processor_data*) {
    vertex* v=reinterpret_cast<vertex*>(p);
    return Mode==1 ? v->parent!=UINT32_MAX && v->phase==phase : phase==0;
  }
  static bool generate_update(unsigned char* p,unsigned char* edge,unsigned char* out,
                              per_processor_data*,unsigned long phase) {
    uint32_t src,dst;
    memcpy(&src,edge,4); memcpy(&dst,edge+4,4);
    vertex* v=reinterpret_cast<vertex*>(p)+x_lib::configuration::map_offset(src);
    if (Mode==1 ? v->parent==UINT32_MAX || v->phase!=phase : phase!=0) return false;
    update* u=reinterpret_cast<update*>(out); u->target=dst;
    if (Mode==2) u->delta=0.85f*(input_scores?input_scores[src]:1.0f/static_cast<float>(n))/static_cast<float>(degrees[src]);
    else u->source=src;
    return true;
  }
  static bool apply_one_update(unsigned char* p,unsigned char* in,per_processor_data*,unsigned long phase) {
    update* u=reinterpret_cast<update*>(in);
    vertex* v=reinterpret_cast<vertex*>(p)+x_lib::configuration::map_offset(u->target);
    if (Mode==1) {
      if(v->parent!=UINT32_MAX) return false;
      v->parent=u->source; v->phase=phase; v->changed=1; return true;
    }
    if (Mode==2) { v->rank+=u->delta; v->changed=1; }
    if (Mode==3) unite(u->source,u->target);
    return false;
  }
  static unsigned long min_super_phases() { return 1; }
  static void observe(unsigned char* p,unsigned long id,per_processor_data* state) {
    vertex* v=reinterpret_cast<vertex*>(p); cpu* c=static_cast<cpu*>(state);
    if (Mode==1) { bool seen=v->parent!=UINT32_MAX; c->count+=seen; if(output) output[id]=seen; }
    if (Mode==2) { c->sum+=v->rank; if(output) reinterpret_cast<float*>(output)[id]=v->rank; }
    if (Mode==3) { uint32_t label=find(id); c->count+=(label==id); if(output) reinterpret_cast<uint32_t*>(output)[id]=label; }
  }
  static void postprocessing() {
    uint64_t count=0; double sum=0;
    for(cpu* c:cpus) { count+=c->count; sum+=c->sum; }
    if (Mode==1) std::cout<<"[e2] bfs_visited="<<count<<std::endl;
    if (Mode==2) std::cout<<"[e2] pr_sum="<<std::setprecision(17)<<sum<<std::endl;
    if (Mode==3) std::cout<<"[e2] cc_components="<<count<<std::endl;
    if(output) { msync(output,output_bytes,MS_SYNC); munmap(output,output_bytes); }
  }
};
}
