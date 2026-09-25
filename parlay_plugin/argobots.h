#ifndef PARLAY_PLUGIN_ARGOBOTS_H_
#define PARLAY_PLUGIN_ARGOBOTS_H_

#include <cstddef>
#include <cstdlib>

#include <functional>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>

#include "ws_runtime.h"

// Число ES: PARLAY_NUM_THREADS, иначе число аппаратных потоков.
// Размер стека ULT: ABT_THREAD_STACKSIZE, иначе PARLAY_ARGOBOTS_STACK_SIZE.
// Если в пуле своего ES уже PARLAY_ARGOBOTS_QUEUE_LIMIT готовых ULT, par_do
// выполняет обе ветки на месте: иначе число живых ULT и их стеков растёт
// с числом листьев дерева задач.
// Метаданные задачи par_do хранятся на стеке вызывающего (он ждёт join),
// а не в куче; PARLAY_ARGOBOTS_STACK_META=0 возвращает выделение в куче.

#ifndef PARLAY_ARGOBOTS_STACK_SIZE
#define PARLAY_ARGOBOTS_STACK_SIZE (1u << 20)
#endif

#ifndef PARLAY_ARGOBOTS_QUEUE_LIMIT
#define PARLAY_ARGOBOTS_QUEUE_LIMIT 8
#endif

namespace parlay {

namespace internal {

inline int argobots_default_num_workers() {
  if (const char* env = std::getenv("PARLAY_NUM_THREADS")) {
    int n = std::atoi(env);
    if (n > 0) return n;
  }
  unsigned n = std::thread::hardware_concurrency();
  return n > 0 ? static_cast<int>(n) : 1;
}

struct argobots_runtime {
  argobots_runtime() {
    ws_runtime_init(argobots_default_num_workers(), PARLAY_ARGOBOTS_STACK_SIZE);
  }
  ~argobots_runtime() { ws_runtime_finalize(); }
};

inline void argobots_ensure_runtime() {
  static argobots_runtime runtime;
}

inline size_t argobots_queue_limit() {
  static const size_t limit = [] {
    if (const char* env = std::getenv("PARLAY_ARGOBOTS_QUEUE_LIMIT")) {
      long v = std::atol(env);
      if (v > 0) return static_cast<size_t>(v);
    }
    return static_cast<size_t>(PARLAY_ARGOBOTS_QUEUE_LIMIT);
  }();
  return limit;
}

inline bool argobots_stack_meta() {
  static const bool enabled = [] {
    const char* env = std::getenv("PARLAY_ARGOBOTS_STACK_META");
    return !(env && env[0] != '\0' && std::atoi(env) == 0);
  }();
  return enabled;
}

template <typename F>
void argobots_invoke(void* p) {
  (*static_cast<F*>(p))();
}

struct argobots_join_guard {
  ABT_thread thread;
  ~argobots_join_guard() { ABT_thread_free(&thread); }
};

template <typename Lf, typename Rf>
inline void argobots_par_do(Lf&& left, Rf&& right, long long estimate) {
  argobots_ensure_runtime();
  using R = std::remove_reference_t<Rf>;
  void* arg = const_cast<void*>(static_cast<const void*>(std::addressof(right)));
  ABT_thread thread = ABT_THREAD_NULL;
  ws_task_meta meta;
  int rank = ws_runtime_self_rank();
  if (rank < 0 || ws_runtime_pool_size(rank) >= argobots_queue_limit() ||
      ws_runtime_spawn_meta(&argobots_invoke<R>, arg, estimate,
                            argobots_stack_meta() ? &meta : nullptr,
                            &thread) != ABT_SUCCESS) {
    std::forward<Lf>(left)();
    std::forward<Rf>(right)();
    return;
  }
  argobots_join_guard guard{thread};
  std::forward<Lf>(left)();
}

template <typename F>
void argobots_parallel_for(size_t start, size_t end, F& f, size_t grain) {
  if (end - start <= grain) {
    for (size_t i = start; i < end; i++) f(i);
    return;
  }
  size_t mid = start + (end - start) / 2;
  argobots_par_do([&] { argobots_parallel_for(start, mid, f, grain); },
                  [&] { argobots_parallel_for(mid, end, f, grain); },
                  static_cast<long long>(end - mid));
}

}  // namespace internal

inline size_t num_workers() {
  internal::argobots_ensure_runtime();
  return static_cast<size_t>(ws_runtime_num_workers());
}

inline size_t worker_id() {
  internal::argobots_ensure_runtime();
  int rank = ws_runtime_self_rank();
  return rank < 0 ? 0 : static_cast<size_t>(rank);
}

template <typename F>
inline void parallel_for(size_t start, size_t end, F&& f, long granularity, bool) {
  static_assert(std::is_invocable_v<F&, size_t>);
  if (end <= start) return;
  size_t n = end - start;
  if (n == 1) {
    f(start);
    return;
  }
  size_t grain;
  if (granularity > 0) {
    grain = static_cast<size_t>(granularity);
  } else {
    size_t parts = 8 * num_workers();
    grain = (n + parts - 1) / parts;
  }
  if (n <= grain) {
    for (size_t i = start; i < end; i++) f(i);
    return;
  }
  internal::argobots_parallel_for(start, end, f, grain);
}

template <typename Lf, typename Rf>
inline void par_do(Lf&& left, Rf&& right, bool) {
  static_assert(std::is_invocable_v<Lf&&>);
  static_assert(std::is_invocable_v<Rf&&>);
  internal::argobots_par_do(std::forward<Lf>(left), std::forward<Rf>(right), 0);
}

template <typename... Fs>
void execute_with_scheduler(Fs...) {
  struct Illegal {};
  static_assert((std::is_same_v<Illegal, Fs> && ...),
                "parlay::execute_with_scheduler is not supported by the Argobots plugin");
}

}  // namespace parlay

#endif  // PARLAY_PLUGIN_ARGOBOTS_H_
