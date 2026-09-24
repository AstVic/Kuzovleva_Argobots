#ifndef PARLAY_PLUGIN_ARGOBOTS_H_
#define PARLAY_PLUGIN_ARGOBOTS_H_

#include <cstddef>

#include <functional>
#include <type_traits>
#include <utility>

namespace parlay {

inline size_t num_workers() { return 1; }
inline size_t worker_id() { return 0; }

template <typename F>
inline void parallel_for(size_t start, size_t end, F&& f, long, bool) {
  static_assert(std::is_invocable_v<F&, size_t>);
  for (size_t i = start; i < end; i++) {
    f(i);
  }
}

template <typename Lf, typename Rf>
inline void par_do(Lf&& left, Rf&& right, bool) {
  static_assert(std::is_invocable_v<Lf&&>);
  static_assert(std::is_invocable_v<Rf&&>);
  std::forward<Lf>(left)();
  std::forward<Rf>(right)();
}

template <typename... Fs>
void execute_with_scheduler(Fs...) {
  struct Illegal {};
  static_assert((std::is_same_v<Illegal, Fs> && ...),
                "parlay::execute_with_scheduler is not supported by the Argobots plugin");
}

}  // namespace parlay

#endif  // PARLAY_PLUGIN_ARGOBOTS_H_
