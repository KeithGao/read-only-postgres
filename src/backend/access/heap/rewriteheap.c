/*-------------------------------------------------------------------------
 *
 * rewriteheap.c
 *	  Support functions to rewrite tables.
 *
 * These functions provide a facility to completely rewrite a heap, while
 * preserving visibility information and update chains.
 *
 * INTERFACE
 *
 * The caller is responsible for creating the new heap, all catalog
 * changes, supplying the tuples to be written to the new heap, and
 * rebuilding indexes.	The caller must hold AccessExclusiveLock on the
 * target table, because we assume no one else is writing into it.
 *
 * To use the facility:
 *
 * begin_heap_rewrite
 * while (fetch next tuple)
 * {
 *	   if (tuple is dead)
 *		   rewrite_heap_dead_tuple
 *	   else
 *	   {
 *		   // do any transformations here if required
 *		   rewrite_heap_tuple
 *	   }
 * }
 * end_heap_rewrite
 *
 * The contents of the new relation shouldn't be relied on until after
 * end_heap_rewrite is called.
 *
 *
 * IMPLEMENTATION
 *
 * This would be a fairly trivial affair, except that we need to maintain
 * the ctid chains that link versions of an updated tuple together.
 * Since the newly stored tuples will have tids different from the original
 * ones, if we just copied t_ctid fields to the new table the links would
 * be wrong.  When we are required to copy a (presumably recently-dead or
 * delete-in-progress) tuple whose ctid doesn't point to itself, we have
 * to substitute the correct ctid instead.
 *
 * For each ctid reference from A -> B, we might encounter either A first
 * or B first.	(Note that a tuple in the middle of a chain is both A and B
 * of different pairs.)
 *
 * If we encounter A first, we'll store the tuple in the unresolved_tups
 * hash table. When we later encounter B, we remove A from the hash table,
 * fix the ctid to point to the new location of B, and insert both A and B
 * to the new heap.
 *
 * If we encounter B first, we can insert B to the new heap right away.
 * We then add an entry to the old_new_tid_map hash table showing B's
 * original tid (in the old heap) and new tid (in the new heap).
 * When we later encounter A, we get the new location of B from the table,
 * and can write A immediately with the correct ctid.
 *
 * Entries in the hash tables can be removed as soon as the later tuple
 * is encountered.	That helps to keep the memory usage down.  At the end,
 * both tables are usually empty; we should have encountered both A and B
 * of each pair.  However, it's possible for A to be RECENTLY_DEAD and B
 * entirely DEAD according to HeapTupleSatisfiesVacuum, because the test
 * for deadness using OldestXmin is not exact.	In such a case we might
 * encounter B first, and skip it, and find A later.  Then A would be added
 * to unresolved_tups, and stay there until end of the rewrite.  Since
 * this case is very unusual, we don't worry about the memory usage.
 *
 * Using in-memory hash tables means that we use some memory for each live
 * update chain in the table, from the time we find one end of the
 * reference until we find the other end.  That shouldn't be a problem in
 * practice, but if you do something like an UPDATE without a where-clause
 * on a large table, and then run CLUSTER in the same transaction, you
 * could run out of memory.  It doesn't seem worthwhile to add support for
 * spill-to-disk, as there shouldn't be that many RECENTLY_DEAD tuples in a
 * table under normal circumstances.  Furthermore, in the typical scenario
 * of CLUSTERing on an unchanging key column, we'll see all the versions
 * of a given tuple together anyway, and so the peak memory usage is only
 * proportional to the number of RECENTLY_DEAD versions of a single row, not
 * in the whole table.	Note that if we do fail halfway through a CLUSTER,
 * the old table is still valid, so failure is not catastrophic.
 *
 * We can't use the normal heap_insert function to insert into the new
 * heap, because heap_insert overwrites the visibility information.
 * We use a special-purpose raw_heap_insert function instead, which
 * is optimized for bulk inserting a lot of tuples, knowing that we have
 * exclusive access to the heap.  raw_heap_insert builds new pages in
 * local storage.  When a page is full, or at the end of the process,
 * we insert it to WAL as a single record and then write it to disk
 * directly through smgr.  Note, however, that any data sent to the new
 * heap's TOAST table will go through the normal bufmgr.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/rewriteheap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/rewriteheap.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"


/*
 * State associated with a rewrite operation. This is opaque to the user
 * of the rewrite facility.
 */
typedef struct RewriteStateData
{
	Relation	rs_new_rel;		/* destination heap */
	Page		rs_buffer;		/* page currently being built */
	BlockNumber rs_blockno;		/* block where page will go */
	bool		rs_buffer_valid;	/* T if any tuples in buffer */
	bool		rs_use_wal;		/* must we WAL-log inserts? */
	TransactionId rs_oldest_xmin;		/* oldest xmin used by caller to
										 * determine tuple visibility */
	TransactionId rs_freeze_xid;/* Xid that will be used as freeze cutoff
								 * point */
	MemoryContext rs_cxt;		/* for hash tables and entries and tuples in
								 * them */
	HTAB	   *rs_unresolved_tups;		/* unmatched A tuples */
	HTAB	   *rs_old_new_tid_map;		/* unmatched B tuples */
}	RewriteStateData;

/*
 * The lookup keys for the hash tables are tuple TID and xmin (we must check
 * both to avoid false matches from dead tuples).  Beware that there is
 * probably some padding space in this struct; it must be zeroed out for
 * correct hashtable operation.
 */
typedef struct
{
	TransactionId xmin;			/* tuple xmin */
	ItemPointerData tid;		/* tuple location in old heap */
} TidHashKey;

/*
 * Entry structures for the hash tables
 */
typedef struct
{
	TidHashKey	key;			/* expected xmin/old location of B tuple */
	ItemPointerData old_tid;	/* A's location in the old heap */
	HeapTuple	tuple;			/* A's tuple contents */
} UnresolvedTupData;

typedef UnresolvedTupData *UnresolvedTup;

typedef struct
{
	TidHashKey	key;			/* actual xmin/old location of B tuple */
	ItemPointerData new_tid;	/* where we put it in the new heap */
} OldToNewMappingData;

typedef OldToNewMappingData *OldToNewMapping;


/* prototypes for internal functions */
static void raw_heap_insert(RewriteState state, HeapTuple tup);


/*
 * Begin a rewrite of a table
 *
 * new_heap		new, locked heap relation to insert tuples to
 * oldest_xmin	xid used by the caller to determine which tuples are dead
 * freeze_xid	xid before which tuples will be frozen
 * use_wal		should the inserts to the new heap be WAL-logged?
 *
 * Returns an opaque RewriteState, allocated in current memory context,
 * to be used in subsequent calls to the other functions.
 */
RewriteState
begin_heap_rewrite(Relation new_heap, TransactionId oldest_xmin,
				   TransactionId freeze_xid, bool use_wal)
{
	RewriteState state;
	MemoryContext rw_cxt;
	MemoryContext old_cxt;
	HASHCTL		hash_ctl;

	/*
	 * To ease cleanup, make a separate context that will contain the
	 * RewriteState struct itself plus all subsidiary data.
	 */
	rw_cxt = AllocSetContextCreate(CurrentMemoryContext,
								   "Table rewrite",
								   ALLOCSET_DEFAULT_MINSIZE,
								   ALLOCSET_DEFAULT_INITSIZE,
								   ALLOCSET_DEFAULT_MAXSIZE);
	old_cxt = MemoryContextSwitchTo(rw_cxt);

	/* Create and fill in the state struct */
	state = palloc0(sizeof(RewriteStateData));

	state->rs_new_rel = new_heap;
	state->rs_buffer = (Page) palloc(BLCKSZ);
	/* new_heap needn't be empty, just locked */
	state->rs_blockno = RelationGetNumberOfBlocks(new_heap);
	state->rs_buffer_valid = false;
	state->rs_use_wal = use_wal;
	state->rs_oldest_xmin = oldest_xmin;
	state->rs_freeze_xid = freeze_xid;
	state->rs_cxt = rw_cxt;

	/* Initialize hash tables used to track update chains */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(TidHashKey);
	hash_ctl.entrysize = sizeof(UnresolvedTupData);
	hash_ctl.hcxt = state->rs_cxt;
	hash_ctl.hash = tag_hash;

	state->rs_unresolved_tups =
		hash_create("Rewrite / Unresolved ctids",
					128,		/* arbitrary initial size */
					&hash_ctl,
					HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	hash_ctl.entrysize = sizeof(OldToNewMappingData);

	state->rs_old_new_tid_map =
		hash_create("Rewrite / Old to new tid map",
					128,		/* arbitrary initial size */
					&hash_ctl,
					HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	MemoryContextSwitchTo(old_cxt);

	return state;
}

/*
 * End a rewrite.
 *
 * state and any other resources are freed.
 */
void
end_heap_rewrite(RewriteState state)
{
	HASH_SEQ_STATUS seq_status;
	UnresolvedTup unresolved;

	/*
	 * Write any remaining tuples in the UnresolvedTups table. If we have any
	 * left, they should in fact be dead, but let's err on the safe side.
	 */
	hash_seq_init(&seq_status, state->rs_unresolved_tups);

	while ((unresolved = hash_seq_search(&seq_status)) != NULL)
	{
		// ItemPointerSetInvalid(&unresolved->tuple->t_data->t_ctid);
		raw_heap_insert(state, unresolved->tuple);
	}

	/* Write the last page, if any */
	if (state->rs_buffer_valid)
	{
		if (state->rs_use_wal)
			log_newpage(&state->rs_new_rel->rd_node,
						MAIN_FORKNUM,
						state->rs_blockno,
						state->rs_buffer);
		RelationOpenSmgr(state->rs_new_rel);
		smgrextend(state->rs_new_rel->rd_smgr, MAIN_FORKNUM, state->rs_blockno,
				   (char *) state->rs_buffer, true);
	}

	/*
	 * If the rel is WAL-logged, must fsync before commit.	We use heap_sync
	 * to ensure that the toast table gets fsync'd too.
	 *
	 * It's obvious that we must do this when not WAL-logging. It's less
	 * obvious that we have to do it even if we did WAL-log the pages. The
	 * reason is the same as in tablecmds.c's copy_relation_data(): we're
	 * writing data that's not in shared buffers, and so a CHECKPOINT
	 * occurring during the rewriteheap operation won't have fsync'd data we
	 * wrote before the checkpoint.
	 */
	if (RelationNeedsWAL(state->rs_new_rel))
		heap_sync(state->rs_new_rel);

	/* Deleting the context frees everything */
	MemoryContextDelete(state->rs_cxt);
}

/*
 * Add a tuple to the new heap.
 *
 * Visibility information is copied from the original tuple, except that
 * we "freeze" very-old tuples.  Note that since we scribble on new_tuple,
 * it had better be temp storage not a pointer to the original tuple.
 *
 * state		opaque state as returned by begin_heap_rewrite
 * old_tuple	original tuple in the old heap
 * new_tuple	new, rewritten tuple to be inserted to new heap
 */
void
rewrite_heap_tuple(RewriteState state,
				   HeapTuple old_tuple, HeapTuple new_tuple)
{
	return;
}

/*
 * Register a dead tuple with an ongoing rewrite. Dead tuples are not
 * copied to the new table, but we still make note of them so that we
 * can release some resources earlier.
 *
 * Returns true if a tuple was removed from the unresolved_tups table.
 * This indicates that that tuple, previously thought to be "recently dead",
 * is now known really dead and won't be written to the output.
 */
bool
rewrite_heap_dead_tuple(RewriteState state, HeapTuple old_tuple)
{
	/*
	 * If we have already seen an earlier tuple in the update chain that
	 * points to this tuple, let's forget about that earlier tuple. It's in
	 * fact dead as well, our simple xmax < OldestXmin test in
	 * HeapTupleSatisfiesVacuum just wasn't enough to detect it. It happens
	 * when xmin of a tuple is greater than xmax, which sounds
	 * counter-intuitive but is perfectly valid.
	 *
	 * We don't bother to try to detect the situation the other way round,
	 * when we encounter the dead tuple first and then the recently dead one
	 * that points to it. If that happens, we'll have some unmatched entries
	 * in the UnresolvedTups hash table at the end. That can happen anyway,
	 * because a vacuum might have removed the dead tuple in the chain before
	 * us.
	 */
	UnresolvedTup unresolved;
	TidHashKey	hashkey;
	bool		found;

	memset(&hashkey, 0, sizeof(hashkey));
	hashkey.xmin = HeapTupleHeaderGetXmin(old_tuple->t_data);
	hashkey.tid = old_tuple->t_self;

	unresolved = hash_search(state->rs_unresolved_tups, &hashkey,
							 HASH_FIND, NULL);

	if (unresolved != NULL)
	{
		/* Need to free the contained tuple as well as the hashtable entry */
		heap_freetuple(unresolved->tuple);
		hash_search(state->rs_unresolved_tups, &hashkey,
					HASH_REMOVE, &found);
		Assert(found);
		return true;
	}

	return false;
}

/*
 * Insert a tuple to the new relation.	This has to track heap_insert
 * and its subsidiary functions!
 *
 * t_self of the tuple is set to the new TID of the tuple. If t_ctid of the
 * tuple is invalid on entry, it's replaced with the new TID as well (in
 * the inserted data only, not in the caller's copy).
 */
static void
raw_heap_insert(RewriteState state, HeapTuple tup)
{
	return;
}
