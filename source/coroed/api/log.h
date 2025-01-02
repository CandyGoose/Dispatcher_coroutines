#pragma once

#include <stdio.h>  // NOLINT: Клиенты должны иметь доступ к printf

#include "coroed/core/spinlock.h"

/**
 * Напечатать сообщение в stdout.
 *
 * Интерфейс аналогичен printf.
 */
#define LOG_INFO(message, ...)                   \
  do {                                           \
    spinlock_lock(&log_lock);                    \
    printf((message)__VA_OPT__(, ) __VA_ARGS__); \
    spinlock_unlock(&log_lock);                  \
  } while (false)

/**
 * Аналогичен `LOG_INFO`, но сбрасывает буффер.
 */
#define LOG_INFO_FLUSHED(message, ...)           \
  do {                                           \
    spinlock_lock(&log_lock);                    \
    printf((message)__VA_OPT__(, ) __VA_ARGS__); \
    fflush(stdout); /* NOLINT */                 \
    spinlock_unlock(&log_lock);                  \
  } while (false)

/**
 * Гарантирует сериализацию сообщений в логе.
 */
extern struct spinlock log_lock;

/**
 * Инициализиировать логгер.
 *
 * Процедура должна быть вызвана ровно один раз
 * в начале программы, если вы хотите сериализовать
 * записи в лог.
 *
 * Важно: логгер не предназначен для использования в
 *        планировщике.
 */
void log_init();
