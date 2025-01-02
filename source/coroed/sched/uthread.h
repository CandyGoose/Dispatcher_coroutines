#pragma once

#include <stdint.h>

/**
 * Файбер. Реализация потока в режиме пользователя.
 */
struct uthread {
  void* context;
};

/**
 * Указатель на функцию, с которой начнет выполнения
 * поток при запуске.
 */
typedef void (*uthread_routine)();

/**
 * Находясь в контексте исполнения потока `prev`,
 * переключиться на контекст исполнения потока `next`.
 */
void uthread_switch(struct uthread* prev, struct uthread* next);

/**
 * Выделить память под хранение данных потока, в том числе стэк.
 */
struct uthread* uthread_allocate();

/**
 * Освободить память, занятую потоком.
 */
void uthread_free(struct uthread* thread);

/**
 * Вернуть поток в изначальное состояние для переиспользования.
 */
void uthread_reset(struct uthread* thread);

/**
 * Установить точку входа. Имеет силу только до первого запуска потока.
 */
void uthread_set_entry(struct uthread* thread, uthread_routine entry);

/**
 * Установить 0-й аргумент, который передается через регистр `r15`,
 * так как именно его мы храним в `switch_frame`.
 */
void uthread_set_arg_0(struct uthread* thread, void* value);

/**
 * Установить 1-й аргумент, который передается через регистр `r14`,
 * так как именно его мы храним в `switch_frame`.
 */
void uthread_set_arg_1(struct uthread* thread, void* value);

/**
 * Получить 0-й аргумент потока. Должен быть вызван лишь один раз
 * в самом начале `uthread_routine`, пока регистр не испортился.
 */
#define UTHREAD_READ_ARG_0(r15) __asm__ volatile("movq %%r15, %0" : "=r"(r15) : : "r15");

/**
 * Получить 1-й аргумент потока. Должен быть вызван лишь один раз
 * в самом начале `uthread_routine`, пока регистр не испортился.
 */
#define UTHREAD_READ_ARG_1(r14) __asm__ volatile("movq %%r14, %0" : "=r"(r14) : : "r14");
