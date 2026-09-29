// Independently inspect native transposed BF rows for every stored source.
#include <immintrin.h>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <vector>
int main(int argc,char** argv) {
  if(argc!=5) return 2;
  FILE* edges=fopen(argv[1],"rb"); FILE* bf=fopen(argv[2],"rb");
  if(!edges||!bf) return 2;
  unsigned bits=atoi(argv[3]),epb=atoi(argv[4]);
  std::vector<unsigned char> group(bits*32);
  uint32_t e[2]; uint64_t idx=0,loaded=UINT64_MAX;
  while(fread(e,8,1,edges)==1) {
    uint64_t page=idx/epb,g=page/256;
    if(g!=loaded) { if(fread(group.data(),1,group.size(),bf)!=group.size()) return 3;loaded=g; }
    uint64_t h1=_mm_crc32_u64(0,e[0]),h2=_mm_crc32_u64(h1,e[0]);
    unsigned hashes[]={unsigned(h1)&(bits-1),unsigned(h1>>16)&(bits-1),unsigned(h2)&(bits-1),unsigned(h2>>16)&(bits-1)};
    for(unsigned h:hashes) if(!(group[h*32+(page%256)/8]&(1u<<(page%8)))) {
      printf("false negative edge=%llu src=%u page=%llu hash=%u\n",(unsigned long long)idx,e[0],(unsigned long long)page,h);return 1;
    }
    ++idx;
  }
  printf("BF zero false negatives: %llu edges\n",(unsigned long long)idx);
}
