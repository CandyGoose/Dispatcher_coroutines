#pragma once

#include <stdatomic.h>
#include <stdbool.h>

/**
 * Примитив синхронизации Spinlock.
 *
 * Позволяет обеспечить взаимное исключение.
 */
struct spinlock {
  atomic_bool is_locked;
};

/**
 * Инициализировать спинлок.
 */
void spinlock_init(struct spinlock* lock);

/**
 * Захватить блокировку.
 *
 * Блокирует текщий поток, пока спинлок занят.
 */
void spinlock_lock(struct spinlock* lock);

/**
 * Попробовать захватить блокировку.
 *
 * Если вернул `true`, то спинлок захвачен,
 * иначе он занят, можно попробовать снова или
 * заняться чем-то полезным, пока лок не освободится.
 */
bool spinlock_try_lock(struct spinlock* lock);

/**
 * Отпустить блокироку.
 */
void spinlock_unlock(struct spinlock* lock);
