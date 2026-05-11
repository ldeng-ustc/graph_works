#ifndef MARCOS_H
#define MARCOS_H

// Macros to disable copying and moving
#define DISALLOW_COPY(cname)                                    \
  cname(const cname &)                   = delete; /* NOLINT */ \
  auto operator=(const cname &)->cname & = delete; /* NOLINT */

#define DISALLOW_MOVE(cname)                               \
  cname(cname &&)                   = delete; /* NOLINT */ \
  auto operator=(cname &&)->cname & = delete; /* NOLINT */

#define DISALLOW_COPY_AND_MOVE(cname) \
  DISALLOW_COPY(cname);               \
  DISALLOW_MOVE(cname);

#define LSMG_ENSURE(expr, message)                    \
  if (!(expr)) {                                      \
    std::cerr << "ERROR: " << (message) << std::endl; \
    std::terminate();                                 \
  }

#define LSMG_ASSERT(expr, message) assert((expr) && (message))

#endif