/*****************************************************************************\
 *  persist.h - slurmbrokerd state checkpointing & restore.
 *****************************************************************************
 *  Part of the Slurm cross-domain broker (MVP). See doc/Broker详细设计文档MVP.md
 *  §4.3/§4.4 and doc/checklists/M03-data-persist.md for design.
 *
 *  This module is the "WAL" of the broker: every g_broker_jobs entry is
 *  rendered to a single JSON line and atomically renamed into place every
 *  checkpoint_interval seconds, or sooner when persist_async_request() is
 *  called by an event-driven hot path.
\*****************************************************************************/

#ifndef _BROKERD_PERSIST_H
#define _BROKERD_PERSIST_H

/*
 * Lifecycle.
 *
 * Call order at startup is fixed by broker_init():
 *   1. broker_job_table_init()
 *   2. broker_state_restore()      -> populates g_broker_jobs from disk
 *   3. persist_thread_start()      -> begins 30s checkpoint loop
 *
 * persist_thread_stop() must run before broker_job_table_fini() so that
 * the worker thread does not race a freed table.
 */
extern int  broker_state_restore(void);
extern int  broker_state_save(void);

extern int  persist_thread_start(void);
extern void persist_thread_stop(void);

/*
 * Hint to the checkpoint thread that an important state change just
 * happened. Returns immediately; actual flush runs on the worker thread
 * at most once per loop iteration. Safe to call from any thread, even
 * while holding g_broker_jobs_lock (signalling does not back-pressure).
 */
extern void persist_async_request(void);

#endif /* _BROKERD_PERSIST_H */
