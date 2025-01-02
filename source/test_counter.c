#include <assert.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "coroed/api/task.h"

static atomic_int_least64_t actual_count = 0;

TASK_DEFINE(increment, void, ignored) {
  atomic_fetch_add(&actual_count, 1);
}

TASK_DEFINE(decrement, void, ignored) {
  atomic_fetch_add(&actual_count, -1);
}

TASK_DEFINE(adder, void, argument) {
  int64_t value = (int64_t)(argument);
  if (0 < value) {
    for (int64_t i = 1; i <= value; ++i) {
      GO(increment, NULL);
    }
  } else if (value < 0) {
    for (int64_t i = -1; i >= value; --i) {
      GO(decrement, NULL);
    }
  }
}

int64_t randint() {
  static const int64_t seed = 213;
  static const int64_t factor = 1103515245;
  static const int64_t summand = 12345;
  static const int64_t modulo = 2147483647;
  static const int64_t limit = 32;

  static int64_t state = seed;

  state = (state * factor + summand) % modulo;
  return (state % (2 * limit + 1)) - limit;
}

void test_counter() {
  static const size_t limit = 16;

  tasks_init();

  int64_t expected_count = 0;

  for (size_t i = 0; i < limit; ++i) {
    const int64_t value = randint();
    expected_count += value;
    tasks_submit(adder, (void*)(value));
  }

  tasks_start();
  tasks_wait();

  assert(atomic_load(&actual_count) == expected_count);

  tasks_destroy();
}
