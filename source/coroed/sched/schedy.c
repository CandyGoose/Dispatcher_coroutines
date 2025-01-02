#include "schedy.h"

#include <assert.h>
#include <sched.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <sys/epoll.h>

#include "coroed/api/task.h"
#include "coroed/core/relax.h"
#include "coroed/core/spinlock.h"
#include "kthread.h"
#include "uthread.h"

static uint64_t now_us() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)(ts.tv_nsec / 1000ULL);
}

enum task_state {
  UTHREAD_RUNNABLE = 0,
  UTHREAD_RUNNING,
  UTHREAD_BLOCKED,
  UTHREAD_FINISHED,
  UTHREAD_ZOMBIE,
  UTHREAD_STATES_COUNT
};

static struct {
  size_t count_in_state[UTHREAD_STATES_COUNT];

  uint64_t total_time_in_state[UTHREAD_STATES_COUNT];

  size_t finished_tasks_count;

  uint64_t min_create_time_us;
  uint64_t max_create_time_us;
  uint64_t sum_create_time_us;
  size_t   created_tasks_count;

  struct spinlock lock;
} g_sched_stats;

struct task {
  struct uthread* thread;
  struct worker* worker;

  enum task_state state;
  struct spinlock lock;

  uint64_t total_running_time_us;
  uint64_t total_runnable_time_us;
  uint64_t total_blocked_time_us;
  uint64_t last_state_change_us;

  uint64_t create_time_us;

  int blocked_fd;
};

struct worker {
  size_t index;
  struct kthread kthread;
  struct uthread sched_thread;
  struct task* running_task;

  struct {
    size_t steps;
    size_t finished;
  } statistics;
};

enum {
  SCHED_THREADS_LIMIT     = 512,
  SCHED_WORKERS_COUNT     = 4,
  SCHED_NEXT_MAX_ATTEMPTS = 16,

//  SCHED_STATS_INTERVAL_US = 5000000,

  MAX_EPOLL_EVENTS = 64
};

static struct task tasks[SCHED_THREADS_LIMIT];
static struct spinlock tasks_lock;
static size_t next_task_index = 0;

static struct worker workers[SCHED_WORKERS_COUNT];
static kthread_id_t kthread_ids[SCHED_WORKERS_COUNT];

static bool stats_thread_running = false;
static struct kthread stats_kthread;

static int g_epoll_fd = -1;
static bool g_epoll_thread_running = false;
static struct kthread g_epoll_kthread;

static uint64_t g_start_time = 0;

static void sched_task_init(struct task* t) {
  t->thread = NULL;
  t->worker = NULL;
  t->state = UTHREAD_ZOMBIE;
  spinlock_init(&t->lock);

  t->total_running_time_us   = 0;
  t->total_runnable_time_us  = 0;
  t->total_blocked_time_us   = 0;
  t->last_state_change_us    = 0;
  t->create_time_us          = 0;

  t->blocked_fd = -1;
}

static void sched_worker_init(struct worker* w, size_t index) {
  w->index = index;
  w->sched_thread.context = NULL;
  w->running_task = NULL;
  w->statistics.steps = 0;
  w->statistics.finished = 0;
}

void sched_init() {
  g_start_time = now_us();

  memset(&g_sched_stats, 0, sizeof(g_sched_stats));
  spinlock_init(&g_sched_stats.lock);

  g_sched_stats.min_create_time_us = UINT64_MAX;
  g_sched_stats.max_create_time_us = 0;
  g_sched_stats.sum_create_time_us = 0;
  g_sched_stats.created_tasks_count = 0;

  spinlock_init(&tasks_lock);
  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    sched_task_init(&tasks[i]);
  }

  next_task_index = 0;
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    kthread_ids[i] = 0;
    sched_worker_init(&workers[i], i);
  }

  g_epoll_fd = epoll_create1(0);
  if (g_epoll_fd < 0) {
    perror("epoll_create1");
    exit(1);
  }
}

static void sched_task_state_transition(struct task* t, enum task_state new_state) {
  const uint64_t now = now_us();

  uint64_t delta = 0;
  if (t->last_state_change_us != 0) {
    delta = now - t->last_state_change_us;
  }

  switch (t->state) {
    case UTHREAD_RUNNING:
      t->total_running_time_us += delta;
      break;
    case UTHREAD_RUNNABLE:
      t->total_runnable_time_us += delta;
      break;
    case UTHREAD_BLOCKED:
      t->total_blocked_time_us += delta;
      break;
    default:
      break;
  }

  spinlock_lock(&g_sched_stats.lock);

  if (g_sched_stats.count_in_state[t->state] > 0) {
    g_sched_stats.count_in_state[t->state] -= 1;
  }
  g_sched_stats.total_time_in_state[t->state] += delta;
  g_sched_stats.count_in_state[new_state] += 1;

  if (new_state == UTHREAD_FINISHED) {
    g_sched_stats.finished_tasks_count++;
    if (t->worker) {
      t->worker->statistics.finished++;
    }
  }

  spinlock_unlock(&g_sched_stats.lock);

  t->state = new_state;
  t->last_state_change_us = now;
}

static void sched_switch_to_scheduler(struct task* task) {
  if (task->state == UTHREAD_RUNNING) {
    sched_task_state_transition(task, UTHREAD_RUNNABLE);
  }

  struct worker* w = task->worker;
  task->worker = NULL;

  uthread_switch(task->thread, &w->sched_thread);
}

static void sched_switch_to(struct worker* w, struct task* t) {
  assert(t->thread != &w->sched_thread);

  sched_task_state_transition(t, UTHREAD_RUNNING);

  t->worker = w;
  w->running_task = t;

  uthread_switch(&w->sched_thread, t->thread);
}

static struct task* sched_acquire_next() {
  for (size_t attempt = 0; attempt < SCHED_NEXT_MAX_ATTEMPTS; ++attempt) {
    spinlock_lock(&tasks_lock);

    for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
      struct task* t = &tasks[next_task_index];
      next_task_index = (next_task_index + 1) % SCHED_THREADS_LIMIT;

      if (!spinlock_try_lock(&t->lock)) {
        continue;
      }

      if (t->thread != NULL && t->state == UTHREAD_RUNNABLE) {
        spinlock_unlock(&tasks_lock);
        return t;
      }
      spinlock_unlock(&t->lock);
    }

    spinlock_unlock(&tasks_lock);
    SPINLOOP(2 * attempt);
  }
  return NULL;
}

static void sched_release(struct task* t) {
  if (t->state == UTHREAD_FINISHED) {
    uthread_reset(t->thread);
    sched_task_state_transition(t, UTHREAD_ZOMBIE);
  }
  spinlock_unlock(&t->lock);
}

static int sched_loop(void* argument) {
  struct worker* w = argument;
  kthread_ids[w->index] = kthread_id();

  while (true) {
    struct task* t = sched_acquire_next();
    if (!t) {
      break;
    }

    sched_switch_to(w, t);
    w->statistics.steps += 1;
    sched_release(t);
  }

  return 0;
}

static int stats_thread_loop(void* arg) {
  (void)arg;
  stats_thread_running = true;

//  while (stats_thread_running) {
//    struct timespec ts;
//    ts.tv_sec  = SCHED_STATS_INTERVAL_US / 1000000;
//    ts.tv_nsec = (SCHED_STATS_INTERVAL_US % 1000000) * 1000;
//    nanosleep(&ts, NULL);
//
//    sched_print_statistics();
//  }

  return 0;
}

static bool g_epoll_thread_should_stop = false;

static int epoll_thread_loop(void* arg) {
  (void)arg;
  g_epoll_thread_running = true;

  struct epoll_event events[MAX_EPOLL_EVENTS];

  while (!g_epoll_thread_should_stop) {
    int n = epoll_wait(g_epoll_fd, events, MAX_EPOLL_EVENTS, 1000);
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      perror("epoll_wait");
      break;
    }

    for (int i = 0; i < n; i++) {
      struct task* t = (struct task*)events[i].data.ptr;
      if (!t) continue;

      if (!spinlock_try_lock(&t->lock)) {
        continue;
      }
      if (t->state == UTHREAD_BLOCKED) {
        sched_task_state_transition(t, UTHREAD_RUNNABLE);
        t->blocked_fd = -1;
      }
      spinlock_unlock(&t->lock);
    }
  }

  g_epoll_thread_running = false;
  return 0;
}

void sched_start() {
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* w = &workers[i];
    enum kthread_status status = kthread_create(&w->kthread, sched_loop, w);
    assert(status == KTHREAD_SUCCESS);
  }

  enum kthread_status stats_status = kthread_create(&stats_kthread, stats_thread_loop, NULL);
  assert(stats_status == KTHREAD_SUCCESS);

  g_epoll_thread_should_stop = false;
  enum kthread_status epoll_status = kthread_create(&g_epoll_kthread, epoll_thread_loop, NULL);
  assert(epoll_status == KTHREAD_SUCCESS);
}

void sched_wait() {
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* w = &workers[i];
    kthread_join(&w->kthread);
  }

  stats_thread_running = false;
  kthread_join(&stats_kthread);

  g_epoll_thread_should_stop = true;
  kthread_join(&g_epoll_kthread);
}

static task_t sched_try_submit(void (*entry)(), void* argument) {
  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    struct task* t = &tasks[i];

    if (!spinlock_try_lock(&t->lock)) {
      continue;
    }

    if (t->state == UTHREAD_ZOMBIE) {
      if (t->thread == NULL) {
        t->thread = uthread_allocate();
        assert(t->thread != NULL);

        t->last_state_change_us = now_us();
        t->create_time_us       = t->last_state_change_us - g_start_time;
      }

      uthread_reset(t->thread);
      uthread_set_entry(t->thread, entry);
      uthread_set_arg_0(t->thread, t);
      uthread_set_arg_1(t->thread, argument);

      sched_task_state_transition(t, UTHREAD_RUNNABLE);

      spinlock_lock(&g_sched_stats.lock);
      g_sched_stats.created_tasks_count++;
      if (t->create_time_us < g_sched_stats.min_create_time_us) {
        g_sched_stats.min_create_time_us = t->create_time_us;
      }
      if (t->create_time_us > g_sched_stats.max_create_time_us) {
        g_sched_stats.max_create_time_us = t->create_time_us;
      }
      g_sched_stats.sum_create_time_us += t->create_time_us;
      spinlock_unlock(&g_sched_stats.lock);

      spinlock_unlock(&t->lock);
      return (task_t){.task = t};
    }

    spinlock_unlock(&t->lock);
  }

  return (task_t){.task = NULL};
}

task_t sched_submit(uthread_routine entry, void* argument) {
  for (size_t attempt = 0; attempt < SCHED_NEXT_MAX_ATTEMPTS; ++attempt) {
    task_t handle = sched_try_submit(entry, argument);
    if (handle.task != NULL) {
      return handle;
    }
    SPINLOOP(2 * attempt);
  }
  assert(false && "Can't create a task: all tasks are busy!");
  return (task_t){.task = NULL};
}

void sched_print_statistics() {
  size_t total_finished = 0;
  size_t total_steps    = 0;
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    total_finished += workers[i].statistics.finished;
    total_steps    += workers[i].statistics.steps;
  }

  printf("\n===== SCHED STATISTICS =====\n");
  printf("Workers finished tasks (summed): %zu\n", total_finished);
  printf("Workers steps (summed):         %zu\n\n", total_steps);

  spinlock_lock(&g_sched_stats.lock);

  printf("Total finished tasks (global):  %zu\n", g_sched_stats.finished_tasks_count);
  printf("Created tasks count:            %zu\n", g_sched_stats.created_tasks_count);

  if (g_sched_stats.created_tasks_count > 0) {
    uint64_t min_us = g_sched_stats.min_create_time_us;
    uint64_t max_us = g_sched_stats.max_create_time_us;
    double avg_us =
        (double)g_sched_stats.sum_create_time_us / (double)g_sched_stats.created_tasks_count;
    printf("Creation time (us) min/avg/max: %llu / %.2f / %llu\n",
           (unsigned long long)min_us,
           avg_us,
           (unsigned long long)max_us);
  }

  static const char* state_names[] = {
      "RUNNABLE", "RUNNING", "BLOCKED", "FINISHED", "ZOMBIE"
  };

  for (int s = 0; s < UTHREAD_STATES_COUNT; s++) {
    printf("\nState %-8s:\n", state_names[s]);
    printf("  |- current tasks in state:  %zu\n", g_sched_stats.count_in_state[s]);
    printf("  |- total time in state (us): %llu\n",
           (unsigned long long)g_sched_stats.total_time_in_state[s]);
  }

  spinlock_unlock(&g_sched_stats.lock);
}

void sched_destroy() {
  close(g_epoll_fd);

  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    struct task* t = &tasks[i];
    spinlock_lock(&t->lock);
    if (t->thread != NULL) {
      uthread_free(t->thread);
      t->thread = NULL;
    }
    spinlock_unlock(&t->lock);
  }
}

void task_block(struct task* t, int fd) {
  sched_task_state_transition(t, UTHREAD_BLOCKED);
  t->blocked_fd = fd;

  struct epoll_event ev;
  memset(&ev, 0, sizeof(ev));
  ev.events   = EPOLLIN | EPOLLET;
  ev.data.ptr = t;

  if (epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
    if (errno != EEXIST) {
      perror("epoll_ctl ADD");
    } else {
      if (epoll_ctl(g_epoll_fd, EPOLL_CTL_MOD, fd, &ev) < 0) {
        perror("epoll_ctl MOD");
      }
    }
  }

  sched_switch_to_scheduler(t);
}

ssize_t nb_read(struct task* caller, int fd, void* buf, size_t count) {
  while (true) {
    ssize_t ret = read(fd, buf, count);
    if (ret < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        task_block(caller, fd);
        continue;
      }
      return -1;
    }
    return ret;
  }
}

void task_yield(struct task* caller) {
  sched_switch_to_scheduler(caller);
}

void task_exit(struct task* caller) {
  sched_task_state_transition(caller, UTHREAD_FINISHED);
  sched_switch_to_scheduler(caller);
}

task_t task_submit(struct task* caller, uthread_routine entry, void* argument) {
  (void)caller;
  return sched_submit(entry, argument);
}
