#pragma once

/**
 * Provides a hint to the processor that the
 * code sequence is a spin-wait loop.
 */
#define CPU_RELAX() __asm__ volatile("pause" ::: "memory") /* NOLINT */

/**
 * Прокрутиться в цикле `spins` раз. Может быть полезен
 * для реализации `backoff` стратегии при ретраях.
 */
#define SPINLOOP(spins)                 \
  do {                                  \
    for (int i = 0; i < (spins); ++i) { \
      CPU_RELAX();                      \
    }                                   \
  } while (false)
