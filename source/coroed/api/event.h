#pragma once

#include <stdatomic.h>

#include "coroed/api/task.h"

/**
 * Синтаксический сахар для использования в теле файбера.
 */
#define EVENT_WAIT(event) event_wait(__self, (event))

/**
 * Примитив синхронихации `Event`.
 *
 * Позволяет долждаться некоторого события
 * в текущем файбере. Событие может произойти
 * не более 1 раза.
 *
 * Пример использования ищите в `test_event.c`.
 */
struct event {
  atomic_bool is_fired;
};

/**
 * Конструктор объекта `Event`.
 *
 * Изначально событие не произошло (`is_fired = false`).
 */
void event_init(struct event* event);

/**
 * Дождаться события `event`, находясь в файбере `caller`.
 *
 * Возврат из функции произойдет только когда `is_fired = true`.
 */
void event_wait(struct task* caller, struct event* event);

/**
 * Отметить событие произошедшим.
 */
void event_fire(struct event* event);
