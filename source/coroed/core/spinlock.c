#include "spinlock.h"

#include <assert.h>
#include <stdatomic.h>

#include "relax.h"

void spinlock_init(struct spinlock* lock) {
  assert(atomic_is_lock_free(&lock->is_locked));
  atomic_store(&lock->is_locked, false);
}

void spinlock_lock(struct spinlock* lock) {
  // Как можно улучшить данный спинлок?

  while (!spinlock_try_lock(lock)) {
    CPU_RELAX();
  }
}

bool spinlock_try_lock(struct spinlock* lock) {
  return atomic_exchange(&lock->is_locked, true) == false;
}

void spinlock_unlock(struct spinlock* lock) {
  atomic_store(&lock->is_locked, false);
}
