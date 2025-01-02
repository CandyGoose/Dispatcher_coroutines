#include "log.h"

#include "coroed/core/spinlock.h"

struct spinlock log_lock;

void log_init() {
  spinlock_init(&log_lock);
}
