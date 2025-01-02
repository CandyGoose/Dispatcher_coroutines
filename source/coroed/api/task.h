#pragma once

#include "coroed/sched/uthread.h"  // uthread_routine, UTHREAD_READ_ARG_{N}

/**
 * Синтаксический сахар для использования в теле файбера.
 */
#define YIELD task_yield(__self)

/**
 * Синтаксический сахар для использования в теле файбера.
 */
#define GO(entry, argument) task_submit(__self, (entry), (argument))

struct task;

/**
 * Хэндл на запущенную асинхронную задачу.
 *
 * Может быть полезен для реализации операции AWAIT(task_t).
 */
typedef struct {
  struct task* task;
} task_t;

typedef uthread_routine task_body;

/**
 * Инициализировать планировщик.
 *
 * Должен быть вызван ровно один раз до вызова
 * `tasks_destroy`.
 */
void tasks_init();

/**
 * Отправить задачу `body` в планировщик с аргументом.
 *
 * Нужен для создания самого первого файбера, который далее
 * породит остальные подзадачи.
 *
 * Должен быть выполнен до `tasks_start`.
 *
 * Клиентский код ответственен за предоставление достаточного
 * времени жизни `argument`.
 */
void tasks_submit(task_body body, void* argument);

/**
 * Запустить планировщик.
 *
 * Аллоцирует и запускает рабочие потоки.
 */
void tasks_start();

/**
 * Дождаться завершения всех задач в планировщике.
 *
 * Деаллоцирует рабочие потоки.
 */
void tasks_wait();

/**
 * Выводит в stdout статистику планировщика и потоков.
 *
 * Должен быть вызыван после `tasks_wait`, но до `tasks_destroy`.
 */
void tasks_print_statistics();

/**
 * Очищает ресурсы, захваченные планировщиком в
 * процессе работы и `tasks_init`.
 *
 * Должен быть вызван после `tasks_wait`.
 */
void tasks_destroy();

/**
 * Отдать управление планировщику, дать путь другим файберам.
 */
void task_yield(struct task* caller);

/**
 * Завершить работу файбера.
 */
void task_exit(struct task* caller);

/**
 * Создать дочерний файбер.
 */
task_t task_submit(struct task* caller, uthread_routine entry, void* argument);

/**
 * Объявить файбер. Используется в заголовочных файлах.
 */
#define TASK_DECLARE(name, type, argument)                                 \
  void task_body_##name(struct task* __self, type* argument); /* NOLINT */ \
  void name()

/**
 * Определить файбер. Используется в файлах с реализацией.
 *
 * Пример использования ищите в тестах.
 */
#define TASK_DEFINE(name, type, argument)      \
  TASK_DECLARE(name, type, argument);          \
                                               \
  void name() {                                \
    struct task* self = NULL;                  \
    type* argument = NULL;        /* NOLINT */ \
    UTHREAD_READ_ARG_0(self);     /* NOLINT */ \
    UTHREAD_READ_ARG_1(argument); /* NOLINT */ \
    task_body_##name(self, argument);          \
    task_exit(self);                           \
  }                                            \
                                               \
  void task_body_##name(struct task* __self, type* argument) /* NOLINT */
