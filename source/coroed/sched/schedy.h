#pragma once

#include "coroed/api/task.h"
#include "uthread.h"

struct task;

void sched_init();

task_t sched_submit(uthread_routine entry, void* argument);

void sched_start();

void sched_wait();

void sched_print_statistics();

void sched_destroy();
