#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "coroed/api/log.h"
#include "coroed/api/sleep.h"
#include "coroed/api/task.h"

enum {
  DELAY = 200,
  COUNT = 8,
};

static atomic_size_t printed_count = 0;

TASK_DEFINE(print_loop, const char, message) {
  const int delay = atoi(message) * DELAY;  // NOLINT

  for (int i = 0; i < COUNT; ++i) {
    char* msg = malloc(sizeof(char) * (strlen(message) + 1));
    strcpy(msg, message);

    LOG_INFO_FLUSHED("%s", msg);
    atomic_fetch_add(&printed_count, 1);

    free(msg);
    SLEEP(delay);
  }
}

TASK_DEFINE(spammer, void, ignored) {
  for (int i = 0; i < COUNT; ++i) {
    GO(&print_loop, "1");
    GO(&print_loop, "1");
    GO(&print_loop, "1");
    GO(&print_loop, "2");
    GO(&print_loop, "2");
    GO(&print_loop, "3");
    GO(&print_loop, "4");
    GO(&print_loop, "5");
  }
}

void test_print() {
  printf("\n");
  tasks_init();
  tasks_submit(&spammer, NULL);
  tasks_start();
  tasks_wait();
  tasks_print_statistics();
  printf("printed_count: %zu\n", printed_count);
  tasks_destroy();
}
