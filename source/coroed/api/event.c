#include "event.h"

#include <stdatomic.h>
#include <stdbool.h>

#include "task.h"

void event_init(struct event* event) {
  atomic_store(&event->is_fired, false);
}

void event_wait(struct task* caller, struct event* event) {
  // Заметим, что текущая реализация является блокирующей,
  // что может приводить к дедлокам при использовании некоторых
  // алгоритмов планирования (например, FIFO). Для решения этой
  // проблемы следует "парковать" файбер в планировщике, отправляя
  // его в а-ля BLOCKED состояние.

  while (!atomic_load(&event->is_fired)) {
    task_yield(caller);
  }
}

void event_fire(struct event* event) {
  atomic_store(&event->is_fired, true);
}
