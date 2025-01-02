#include <assert.h>
#include <stdatomic.h>
#include <stdlib.h>

#include "coroed/api/event.h"
#include "coroed/api/sleep.h"
#include "coroed/api/task.h"

static atomic_int_least64_t state = 0;
static struct event event;

TASK_DEFINE(eventer, void, ignored) {
  SLEEP(1000);
  state = 1;
  event_fire(&event);
}

TASK_DEFINE(event_test, void, ignored) {
  assert(state == 0);
  GO(eventer, NULL);
  EVENT_WAIT(&event);
  assert(state == 1);
}

void test_event() {
  tasks_init();
  tasks_submit(&event_test, NULL);
  tasks_start();
  tasks_wait();
  tasks_destroy();
  assert(state == 1);
}
