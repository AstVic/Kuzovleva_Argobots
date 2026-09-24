#include <parlay/parallel.h>
#include <parlay/primitives.h>
#include <parlay/random.h>
#include <parlay/sequence.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <numeric>
#include <random>
#include <vector>

static int failures = 0;

static void check(bool ok, const char* name) {
  std::printf("%-40s %s\n", name, ok ? "PASSED" : "FAILED");
  if (!ok) failures++;
}

static long fib_seq(int n) { return n < 2 ? n : fib_seq(n - 1) + fib_seq(n - 2); }

static long fib_par(int n) {
  if (n < 16) return fib_seq(n);
  long a = 0, b = 0;
  parlay::par_do([&] { a = fib_par(n - 1); }, [&] { b = fib_par(n - 2); });
  return a + b;
}

static void test_parallel_for() {
  const size_t n = 10000000;
  std::vector<long> v(n, 0);
  parlay::parallel_for(0, n, [&](size_t i) { v[i] = static_cast<long>(i) * 3 + 1; });
  bool ok = true;
  for (size_t i = 0; i < n; i++) ok &= (v[i] == static_cast<long>(i) * 3 + 1);
  check(ok, "parallel_for writes every index once");

  std::vector<std::atomic<int>> hits(n);
  for (auto& h : hits) h.store(0, std::memory_order_relaxed);
  parlay::parallel_for(0, n, [&](size_t i) { hits[i].fetch_add(1, std::memory_order_relaxed); }, 1000);
  ok = true;
  for (size_t i = 0; i < n; i++) ok &= (hits[i].load() == 1);
  check(ok, "parallel_for with explicit granularity");

  std::atomic<long> empty_calls{0};
  parlay::parallel_for(5, 5, [&](size_t) { empty_calls++; });
  parlay::parallel_for(7, 8, [&](size_t i) { empty_calls += static_cast<long>(i); });
  check(empty_calls.load() == 7, "parallel_for on empty and single ranges");
}

static void test_par_do() {
  check(fib_par(30) == fib_seq(30), "recursive par_do (fib 30)");

  const size_t n = 1000000;
  std::vector<long> left(n), right(n);
  parlay::par_do(
      [&] { parlay::parallel_for(0, n, [&](size_t i) { left[i] = static_cast<long>(i); }); },
      [&] { parlay::parallel_for(0, n, [&](size_t i) { right[i] = static_cast<long>(n - i); }); });
  bool ok = true;
  for (size_t i = 0; i < n; i++) ok &= (left[i] == static_cast<long>(i) && right[i] == static_cast<long>(n - i));
  check(ok, "parallel_for nested in par_do");
}

static void test_worker_id() {
  const size_t p = parlay::num_workers();
  const size_t n = 1000000;
  std::vector<size_t> ids(n);
  parlay::parallel_for(0, n, [&](size_t i) { ids[i] = parlay::worker_id(); }, 1000);
  bool ok = true;
  for (size_t i = 0; i < n; i++) ok &= (ids[i] < p);
  check(ok, "worker_id in [0, num_workers)");
}

static void test_primitives() {
  const size_t n = 2000000;
  parlay::random_generator gen(42);
  std::uniform_int_distribution<long> dis(0, 1000000000);
  auto data = parlay::tabulate(n, [&](size_t i) {
    auto r = gen[i];
    return dis(r);
  });
  std::vector<long> ref(data.begin(), data.end());

  std::sort(ref.begin(), ref.end());
  auto sorted = parlay::sort(data);
  check(std::equal(sorted.begin(), sorted.end(), ref.begin()), "parlay::sort matches std::sort");

  long sum_ref = std::accumulate(ref.begin(), ref.end(), 0L);
  check(parlay::reduce(data) == sum_ref, "parlay::reduce matches std::accumulate");

  auto [prefix, total] = parlay::scan(data);
  bool ok = (total == sum_ref);
  long acc = 0;
  for (size_t i = 0; i < n && ok; i++) {
    ok &= (prefix[i] == acc);
    acc += data[i];
  }
  check(ok, "parlay::scan matches sequential prefix sums");
}

int main() {
  std::printf("num_workers = %zu\n", parlay::num_workers());
  test_parallel_for();
  test_par_do();
  test_worker_id();
  test_primitives();
  std::printf("RESULT: %s\n", failures == 0 ? "PASSED" : "FAILED");
  return failures == 0 ? 0 : 1;
}
