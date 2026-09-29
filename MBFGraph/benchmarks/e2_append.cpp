// Append bin32 batches without sorting, preserving multiplicity and arrival order.
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <string>
#include <fstream>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/resource.h>
#include <sys/prctl.h>
#include <fcntl.h>
#include <unistd.h>
struct Edge { uint32_t src,dst; };
static void write_all(int fd,const void* data,size_t bytes) {
  const char* p=static_cast<const char*>(data);
  while(bytes) { ssize_t count=write(fd,p,bytes); if(count<=0) throw std::runtime_error("write failed"); p+=count;bytes-=count; }
}
int main(int argc,char** argv) { try {
  prctl(PR_SET_DUMPABLE,0); rlimit lim{0,0};setrlimit(RLIMIT_CORE,&lim);
  if(argc!=5) throw std::runtime_error("e2_append INPUT VERTICES OFFSET_EDGES COUNT_EDGES (cwd = graph directory)");
  uint64_t n=std::stoull(argv[2]), offset=std::stoull(argv[3]), count=std::stoull(argv[4]);
  if(!n || n>UINT32_MAX) throw std::runtime_error("invalid vertex count");
  std::vector<uint32_t> degree(n,0);
  struct stat st{}; uint64_t old_edges=0; uint32_t first=0;
  if(!stat("graph",&st)) {
    old_edges=st.st_size/8;
    std::ifstream f("degrees",std::ios::binary);
    if(!f.read(reinterpret_cast<char*>(degree.data()),n*4)) throw std::runtime_error("missing old degrees");
    std::ifstream old("graph",std::ios::binary); old.read(reinterpret_cast<char*>(&first),4);
  }
  FILE* input=fopen(argv[1],"rb"); if(!input) throw std::runtime_error("input open failed");
  if(fseeko(input,offset*8,SEEK_SET)) throw std::runtime_error("input seek failed");
  int out=open("graph",O_WRONLY|O_CREAT|O_APPEND,0600); if(out<0) throw std::runtime_error("output open failed");
  std::vector<Edge> buffer(1<<20); uint64_t done=0;
  while(done<count) {
    size_t take=std::min<uint64_t>(buffer.size(),count-done);
    if(fread(buffer.data(),8,take,input)!=take) throw std::runtime_error("short input");
    for(size_t i=0;i<take;++i) {
      auto e=buffer[i]; if(e.src>=n||e.dst>=n) throw std::runtime_error("endpoint outside domain");
      if(degree[e.src]==UINT32_MAX) throw std::runtime_error("outdegree overflow");
      ++degree[e.src];
    }
    if(old_edges==0&&done==0) first=buffer[0].src;
    write_all(out,buffer.data(),take*8); done+=take;
  }
  if(fsync(out)) throw std::runtime_error("graph fsync failed");close(out);fclose(input);
  int fd=open("degrees",O_WRONLY|O_CREAT|O_TRUNC,0600);
  write_all(fd,degree.data(),n*4);if(fsync(fd)) throw std::runtime_error("degree fsync failed");close(fd);
  std::ofstream ini("graph.ini");ini<<"[graph]\nname=graph\ntype=2\nvertices="<<n<<"\nedges="<<old_edges+count<<"\n";ini.close();
  std::ofstream meta("e2_meta.json");meta<<"{\"edges\":"<<old_edges+count<<",\"root\":"<<first<<"}\n";meta.close();
  printf("[mbf] append old_edges=%llu appended=%llu total=%llu sorted=0\n",(unsigned long long)old_edges,(unsigned long long)count,(unsigned long long)(old_edges+count));
  return 0;
} catch(const std::exception& e) { fprintf(stderr,"e2_append: %s\n",e.what());return 2; } }
