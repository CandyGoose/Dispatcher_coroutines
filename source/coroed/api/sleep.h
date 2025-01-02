#pragma once

#include <stdint.h>

#include "task.h"

/**
 * Синтаксический сахар для использования в теле файбера.
 */
#define SLEEP(ms) task_sleep(__self, (ms))

/**
 * Ожидать не менее заданного количества миллисекунд.
 *
 * Пример использования ищите в `test_event.c`.
 */
void task_sleep(struct task* caller, uint64_t milliseconds);
