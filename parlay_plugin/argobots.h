#ifndef PARLAY_PLUGIN_ARGOBOTS_H_
#define PARLAY_PLUGIN_ARGOBOTS_H_

#include <atomic>
#include <chrono>
#include <climits>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>

#include <functional>
#include <memory>
#include <string_view>
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

inline void argobots_noop(void*) {}

inline long argobots_measure_fork_ns() {
  using clk = std::chrono::steady_clock;
  long best = LONG_MAX;
  for (int rep = 0; rep < 3; rep++) {
    auto t0 = clk::now();
    for (int i = 0; i < 64; i++) {
      ws_task_meta meta;
      ABT_thread thread = ABT_THREAD_NULL;
      if (ws_runtime_spawn_meta(&argobots_noop, nullptr, 0, &meta, &thread) == ABT_SUCCESS) {
        ABT_thread_free(&thread);
      }
    }
    long ns = static_cast<long>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - t0).count() / 64);
    if (ns < best) best = ns;
  }
  return best > 0 ? best : 1;
}

inline std::atomic<long>& argobots_fork_ns_slot() {
  static std::atomic<long> ns{0};
  return ns;
}

struct argobots_runtime {
  argobots_runtime() {
    ws_runtime_init(argobots_default_num_workers(), PARLAY_ARGOBOTS_STACK_SIZE);
    argobots_fork_ns_slot().store(argobots_measure_fork_ns(), std::memory_order_relaxed);
  }
  ~argobots_runtime() { ws_runtime_finalize(); }
};

inline void argobots_ensure_runtime() {
  static argobots_runtime runtime;
}

inline long argobots_fork_ns() {
  argobots_ensure_runtime();
  return argobots_fork_ns_slot().load(std::memory_order_relaxed);
}

inline bool argobots_grain_probe() {
  static const bool probe = [] {
    const char* env = std::getenv("PARLAY_ARGOBOTS_GRAIN");
    return !(env && std::string_view(env) == "fixed");
  }();
  return probe;
}

inline long argobots_target_ns() {
  static const long target = [] {
    long factor = 10;
    if (const char* env = std::getenv("PARLAY_ARGOBOTS_PROBE_FACTOR")) {
      long v = std::atol(env);
      if (v > 0) factor = v;
    }
    return factor * argobots_fork_ns();
  }();
  return target;
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

inline double argobots_grain_factor() {
  static const double factor = [] {
    if (const char* env = std::getenv("PARLAY_ARGOBOTS_GRAIN_FACTOR")) {
      double v = std::strtod(env, nullptr);
      if (v > 0) return v;
    }
    return 1.0;
  }();
  return factor;
}

inline size_t argobots_min_grain() {
  static const size_t grain = [] {
    if (const char* env = std::getenv("PARLAY_ARGOBOTS_MIN_GRAIN")) {
      long v = std::atol(env);
      if (v > 0) return static_cast<size_t>(v);
    }
    return static_cast<size_t>(0);
  }();
  return grain;
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

struct argobots_stats {
  const char* path = std::getenv("PARLAY_ARGOBOTS_STATS");
  std::atomic<long> spawned{0};
  std::atomic<long> live{0};
  std::atomic<long> peak{0};

  void on_spawn() {
    spawned.fetch_add(1, std::memory_order_relaxed);
    long now = live.fetch_add(1, std::memory_order_relaxed) + 1;
    long seen = peak.load(std::memory_order_relaxed);
    while (now > seen && !peak.compare_exchange_weak(seen, now, std::memory_order_relaxed)) {
    }
  }

  void on_join() { live.fetch_sub(1, std::memory_order_relaxed); }

  ~argobots_stats() {
    if (!path || path[0] == '\0') return;
    if (FILE* f = std::fopen(path, "a")) {
      std::fprintf(f, "prog=%s spawned=%ld peak_live=%ld fork_ns=%ld\n", ws_runtime_prog_name(),
                   spawned.load(), peak.load(), argobots_fork_ns_slot().load());
      std::fclose(f);
    }
  }
};

inline argobots_stats& argobots_get_stats() {
  static argobots_stats stats;
  return stats;
}

inline bool argobots_stats_enabled() {
  static const bool enabled = [] {
    const char* env = std::getenv("PARLAY_ARGOBOTS_STATS");
    return env && env[0] != '\0';
  }();
  return enabled;
}

struct argobots_join_guard {
  ABT_thread thread;
  bool counted;
  ~argobots_join_guard() {
    ABT_thread_free(&thread);
    if (counted) argobots_get_stats().on_join();
  }
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
  bool counted = argobots_stats_enabled();
  if (counted) argobots_get_stats().on_spawn();
  argobots_join_guard guard{thread, counted};
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
  if (granularity <= 0 && internal::argobots_grain_probe()) {
    using clk = std::chrono::steady_clock;
    const long target = internal::argobots_target_ns();
    size_t done = 0;
    size_t sz = 1;
    while (true) {
      if (sz > n - done) sz = n - done;
      auto t0 = clk::now();
      for (size_t i = start + done; i < start + done + sz; i++) f(i);
      long ns = static_cast<long>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now() - t0).count());
      done += sz;
      if (done == n) return;
      if (ns >= target) break;
      sz *= 2;
    }
    internal::argobots_parallel_for(start + done, end, f, done);
    return;
  }
  size_t grain;
  if (granularity > 0) {
    grain = static_cast<size_t>(granularity);
  } else {
    size_t parts = 8 * num_workers();
    double factor = internal::argobots_grain_factor();
    if (factor == 1.0) {
      grain = (n + parts - 1) / parts;
    } else {
      grain = static_cast<size_t>(std::ceil(static_cast<double>(n) * factor / static_cast<double>(parts)));
      if (grain == 0) grain = 1;
    }
    size_t min_grain = internal::argobots_min_grain();
    if (grain < min_grain) grain = min_grain;
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
