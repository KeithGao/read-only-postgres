/*-------------------------------------------------------------------------
 *
 * heapam.c
 *	  heap access method code
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam.c
 *
 *
 * INTERFACE ROUTINES
 *		relation_open	- open any relation by relation OID
 *		relation_openrv - open any relation specified by a RangeVar
 *		relation_close	- close any relation
 *		heap_open		- open a heap relation by relation OID
 *		heap_openrv		- open a heap relation specified by a RangeVar
 *		heap_close		- (now just a macro for relation_close)
 *		heap_beginscan	- begin relation scan
 *		heap_rescan		- restart a relation scan
 *		heap_endscan	- end relation scan
 *		heap_getnext	- retrieve next tuple in scan
 *		heap_fetch		- retrieve tuple with given tid
 *		heap_insert		- insert tuple into a relation
 *		heap_multi_insert - insert multiple tuples into a relation
 *		heap_delete		- delete a tuple from a relation
 *		heap_update		- replace a tuple in a relation with another tuple
 *		heap_markpos	- mark scan position
 *		heap_restrpos	- restore position to marked location
 *		heap_sync		- sync heap, for when no WAL has been written
 *
 * NOTES
 *	  This file contains the heap_ routines which implement
 *	  the POSTGRES heap access method used for all POSTGRES
 *	  relations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/hio.h"
#include "access/multixact.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "access/tuptoaster.h"
#include "access/valid.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "access/xlogutils.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "storage/standby.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"


/* GUC variable */
bool		synchronize_seqscans = true;


static HeapScanDesc heap_beginscan_internal(Relation relation,
						Snapshot snapshot,
						int nkeys, ScanKey key,
						bool allow_strat, bool allow_sync,
						bool is_bitmapscan);
static HeapTuple heap_prepare_insert(Relation relation, HeapTuple tup,
					TransactionId xid, CommandId cid, int options);
static XLogRecPtr log_heap_update(Relation reln, Buffer oldbuf,
				ItemPointerData from, Buffer newbuf, HeapTuple newtup,
				bool all_visible_cleared, bool new_all_visible_cleared);
static bool HeapSatisfiesHOTUpdate(Relation relation, Bitmapset *hot_attrs,
					   HeapTuple oldtup, HeapTuple newtup);


/* ----------------------------------------------------------------
 *						 heap support routines
 * ----------------------------------------------------------------
 */

/* ----------------
 *		initscan - scan code common to heap_beginscan and heap_rescan
 * ----------------
 */
static void
initscan(HeapScanDesc scan, ScanKey key, bool is_rescan)
{
	bool		allow_strat;
	bool		allow_sync;

	/*
	 * Determine the number of blocks we have to scan.
	 *
	 * It is sufficient to do this once at scan start, since any tuples added
	 * while the scan is in progress will be invisible to my snapshot anyway.
	 * (That is not true when using a non-MVCC snapshot.  However, we couldn't
	 * guarantee to return tuples added after scan start anyway, since they
	 * might go into pages we already scanned.	To guarantee consistent
	 * results for a non-MVCC snapshot, the caller must hold some higher-level
	 * lock that ensures the interesting tuple(s) won't change.)
	 */
	scan->rs_nblocks = RelationGetNumberOfBlocks(scan->rs_rd);

	/*
	 * If the table is large relative to NBuffers, use a bulk-read access
	 * strategy and enable synchronized scanning (see syncscan.c).	Although
	 * the thresholds for these features could be different, we make them the
	 * same so that there are only two behaviors to tune rather than four.
	 * (However, some callers need to be able to disable one or both of these
	 * behaviors, independently of the size of the table; also there is a GUC
	 * variable that can disable synchronized scanning.)
	 *
	 * During a rescan, don't make a new strategy object if we don't have to.
	 */
	if (!RelationUsesLocalBuffers(scan->rs_rd) &&
		scan->rs_nblocks > NBuffers / 4)
	{
		allow_strat = scan->rs_allow_strat;
		allow_sync = scan->rs_allow_sync;
	}
	else
		allow_strat = allow_sync = false;

	if (allow_strat)
	{
		if (scan->rs_strategy == NULL)
			scan->rs_strategy = GetAccessStrategy(BAS_BULKREAD);
	}
	else
	{
		if (scan->rs_strategy != NULL)
			FreeAccessStrategy(scan->rs_strategy);
		scan->rs_strategy = NULL;
	}

	if (is_rescan)
	{
		/*
		 * If rescan, keep the previous startblock setting so that rewinding a
		 * cursor doesn't generate surprising results.  Reset the syncscan
		 * setting, though.
		 */
		scan->rs_syncscan = (allow_sync && synchronize_seqscans);
	}
	else if (allow_sync && synchronize_seqscans)
	{
		scan->rs_syncscan = true;
		scan->rs_startblock = ss_get_location(scan->rs_rd, scan->rs_nblocks);
	}
	else
	{
		scan->rs_syncscan = false;
		scan->rs_startblock = 0;
	}

	scan->rs_inited = false;
	scan->rs_ctup.t_data = NULL;
	ItemPointerSetInvalid(&scan->rs_ctup.t_self);
	scan->rs_cbuf = InvalidBuffer;
	scan->rs_cblock = InvalidBlockNumber;

	/* we don't have a marked position... */
	ItemPointerSetInvalid(&(scan->rs_mctid));

	/* page-at-a-time fields are always invalid when not rs_inited */

	/*
	 * copy the scan key, if appropriate
	 */
	if (key != NULL)
		memcpy(scan->rs_key, key, scan->rs_nkeys * sizeof(ScanKeyData));

	/*
	 * Currently, we don't have a stats counter for bitmap heap scans (but the
	 * underlying bitmap index scans will be counted).
	 */
	if (!scan->rs_bitmapscan)
		pgstat_count_heap_scan(scan->rs_rd);
}

/*
 * heapgetpage - subroutine for heapgettup()
 *
 * This routine reads and pins the specified page of the relation.
 * In page-at-a-time mode it performs additional work, namely determining
 * which tuples on the page are visible.
 */
static void
heapgetpage(HeapScanDesc scan, BlockNumber page)
{
	Buffer		buffer;
	Snapshot	snapshot;
	Page		dp;
	int			lines;
	int			ntup;
	OffsetNumber lineoff;
	ItemId		lpp;
	bool		all_visible;

	Assert(page < scan->rs_nblocks);

	/* release previous scan buffer, if any */
	if (BufferIsValid(scan->rs_cbuf))
	{
		ReleaseBuffer(scan->rs_cbuf);
		scan->rs_cbuf = InvalidBuffer;
	}

	/*
	 * Be sure to check for interrupts at least once per page.	Checks at
	 * higher code levels won't be able to stop a seqscan that encounters many
	 * pages' worth of consecutive dead tuples.
	 */
	CHECK_FOR_INTERRUPTS();

	/* read page using selected strategy */
	scan->rs_cbuf = ReadBufferExtended(scan->rs_rd, MAIN_FORKNUM, page,
									   RBM_NORMAL, scan->rs_strategy);
	scan->rs_cblock = page;
}

/* ----------------
 *		heapgettup - fetch next heap tuple
 *
 *		Initialize the scan if not already done; then advance to the next
 *		tuple as indicated by "dir"; return the next tuple in scan->rs_ctup,
 *		or set scan->rs_ctup.t_data = NULL if no more tuples.
 *
 * dir == NoMovementScanDirection means "re-fetch the tuple indicated
 * by scan->rs_ctup".
 *
 * Note: the reason nkeys/key are passed separately, even though they are
 * kept in the scan descriptor, is that the caller may not want us to check
 * the scankeys.
 *
 * Note: when we fall off the end of the scan in either direction, we
 * reset rs_inited.  This means that a further request with the same
 * scan direction will restart the scan, which is a bit odd, but a
 * request with the opposite scan direction will start a fresh scan
 * in the proper direction.  The latter is required behavior for cursors,
 * while the former case is generally undefined behavior in Postgres
 * so we don't care too much.
 * ----------------
 */
static void
heapgettup(HeapScanDesc scan,
		   ScanDirection dir,
		   int nkeys,
		   ScanKey key)
{
	HeapTuple	tuple = &(scan->rs_ctup);
	Snapshot	snapshot = scan->rs_snapshot;
	bool		backward = ScanDirectionIsBackward(dir);
	BlockNumber page;
	bool		finished;
	Page		dp;
	ItemId     lpp;
	OffsetNumber tupleidx;
	int lines;
	int         linesleft;

	/*
	int			lines;
	OffsetNumber lineoff;
	int			linesleft;
	*/


	/*
	 * calculate next starting lineoff, given scan direction
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}
			page = scan->rs_startblock; /* first page */
			heapgetpage(scan, page);
			tupleidx = FirstOffsetNumber;
			scan->rs_inited = true;
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock;		/* current page */
			tupleidx = OffsetNumberNext(ItemPointerGetOffsetNumber(&(tuple->t_self)));
		}

		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lines = PageGetMaxOffsetNumber(dp);
		/* page and lineoff now reference the physically next tid */
		linesleft = lines - tupleidx + 1;
	}
	else if (backward)
	{
		if (!scan->rs_inited)
		{
			/*
			 * return null immediately if relation is empty
			 */
			if (scan->rs_nblocks == 0)
			{
				Assert(!BufferIsValid(scan->rs_cbuf));
				tuple->t_data = NULL;
				return;
			}

			/*
			 * Disable reporting to syncscan logic in a backwards scan; it's
			 * not very likely anyone else is doing the same thing at the same
			 * time, and much more likely that we'll just bollix things for
			 * forward scanners.
			 */
			scan->rs_syncscan = false;
			/* start from last page of the scan */
			if (scan->rs_startblock > 0)
				page = scan->rs_startblock - 1;
			else
				page = scan->rs_nblocks - 1;
			heapgetpage(scan, page);
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock;		/* current page */
		}


		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lines = PageGetMaxOffsetNumber(dp);

		if (!scan->rs_inited)
		{
			tupleidx = lines;	/* final offnum */
			scan->rs_inited = true;
		}
		else
		{
			tupleidx = 	OffsetNumberPrev(ItemPointerGetOffsetNumber(&(tuple->t_self)));
		}
		/* page and lineoff now reference the physically previous tid */
		linesleft = tupleidx;
	}
	else
	{
		/*
		 * ``no movement'' scan direction: refetch prior tuple
		 */
		if (!scan->rs_inited)
		{
			Assert(!BufferIsValid(scan->rs_cbuf));
			tuple->t_data = NULL;
			return;
		}

		page = ItemPointerGetBlockNumber(&(tuple->t_self));
		if (page != scan->rs_cblock)
			heapgetpage(scan, page);

		/* Since the tuple was previously fetched, needn't lock page here */
		dp = (Page) BufferGetPage(scan->rs_cbuf);
		tupleidx = ItemPointerGetOffsetNumber(&(tuple->t_self));
		lpp = PageGetItemId(dp, tupleidx);
		Assert(ItemIdIsNormal(lpp));
		tuple->t_data = (HeapTupleHeader) PageGetItem((Page) dp, lpp);

		return;
	}

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	lpp = PageGetItemId(dp, tupleidx);
	for (;;)
	{
		while (linesleft > 0)
		{
			if (ItemIdIsNormal(lpp))
			{
				bool		valid;
				tuple->t_data = (HeapTupleStart) PageGetItem((Page) dp, lpp);
				tuple->t_len = ItemIdGetLength(lpp);
				ItemPointerSet(&(tuple->t_self), page, tupleidx);

				/*
				 * if current tuple qualifies, return it.
				 */
				
				valid = true;

				if (key != NULL)
					HeapKeyTest(tuple, RelationGetDescr(scan->rs_rd),
								nkeys, key, valid);

				if (valid) return;
			}

			 /*
			 * otherwise move to the next item on the page
			 */
			 --linesleft;
			 if (backward)
			 {
			 --lpp;	/* move back in this page's ItemId array */
			 --tupleidx;
			 }
			 else
			 {
			 ++lpp;	/* move forward in this page's ItemId array */
			 ++tupleidx;
			 }
		}

		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */

		/*
		 * advance to next/prior page and detect end of scan
		 */
		if (backward)
		{
			finished = (page == scan->rs_startblock);
			if (page == 0)
				page = scan->rs_nblocks;
			page--;
		}
		else
		{
			page++;
			if (page >= scan->rs_nblocks)
				page = 0;
			finished = (page == scan->rs_startblock);

			/*
			 * Report our new scan position for synchronization purposes. We
			 * don't do that when moving backwards, however. That would just
			 * mess up any other forward-moving scanners.
			 *
			 * Note: we do this before checking for end of scan so that the
			 * final state of the position hint is back at the start of the
			 * rel.  That's not strictly necessary, but otherwise when you run
			 * the same query multiple times the starting position would shift
			 * a little bit backwards on every invocation, which is confusing.
			 * We don't guarantee any specific ordering in general, though.
			 */
			if (scan->rs_syncscan)
				ss_report_location(scan->rs_rd, page);
		}

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (finished)
		{
			if (BufferIsValid(scan->rs_cbuf))
				ReleaseBuffer(scan->rs_cbuf);
			scan->rs_cbuf = InvalidBuffer;
			scan->rs_cblock = InvalidBlockNumber;
			tuple->t_data = NULL;
			scan->rs_inited = false;
			return;
		}

		heapgetpage(scan, page);

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		dp = (Page) BufferGetPage(scan->rs_cbuf);
		lines = PageGetMaxOffsetNumber((Page) dp);
		linesleft = lines;
		if (backward)
		{
			tupleidx = lines;
			lpp = PageGetItemId(dp, lines);
		}
		else
		{
			tupleidx = FirstOffsetNumber;
			lpp = PageGetItemId(dp, FirstOffsetNumber);
		}
	}
}

/* ----------------
 *		heapgettup_pagemode - fetch next heap tuple in page-at-a-time mode
 *
 *		Same API as heapgettup, but used in page-at-a-time mode
 *
 * The internal logic is much the same as heapgettup's too, but there are some
 * differences: we do not take the buffer content lock (that only needs to
 * happen inside heapgetpage), and we iterate through just the tuples listed
 * in rs_vistuples[] rather than all tuples on the page.  Notice that
 * lineindex is 0-based, where the corresponding loop variable lineoff in
 * heapgettup is 1-based.
 * ----------------
 */
static void
heapgettup_pagemode(HeapScanDesc scan,
					ScanDirection dir,
					int nkeys,
					ScanKey key)
{	
	heapgettup(scan, dir, nkeys, key);
}


#if defined(DISABLE_COMPLEX_MACRO)
/*
 * This is formatted so oddly so that the correspondence to the macro
 * definition in access/htup.h is maintained.
 */
Datum
fastgetattr(HeapTuple tup, int attnum, TupleDesc tupleDesc,
			bool *isnull)
{
	if (attnum > 0) {
		if (heap_attisnull(tup, attnum)) {
			*isnull = true;
			return (Datum)NULL;
		} else {
			*isnull = false;
			return fetchatt(tupleDesc->attrs[attnum - 1], tup->t_data);
		}
	}
}
#endif   /* defined(DISABLE_COMPLEX_MACRO) */


/* ----------------------------------------------------------------
 *					 heap access method interface
 * ----------------------------------------------------------------
 */

/* ----------------
 *		relation_open - open any relation by relation OID
 *
 *		If lockmode is not "NoLock", the specified kind of lock is
 *		obtained on the relation.  (Generally, NoLock should only be
 *		used if the caller knows it has some appropriate lock on the
 *		relation already.)
 *
 *		An error is raised if the relation does not exist.
 *
 *		NB: a "relation" is anything with a pg_class entry.  The caller is
 *		expected to check whether the relkind is something it can handle.
 * ----------------
 */
Relation
relation_open(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* Get the lock before trying to open the relcache entry */
	if (lockmode != NoLock)
		LockRelationOid(relationId, lockmode);

	/* The relcache does all the real work... */
	r = RelationIdGetRelation(relationId);

	if (!RelationIsValid(r))
		elog(ERROR, "could not open relation with OID %u", relationId);

	/* Make note that we've accessed a temporary relation */
	if (RelationUsesLocalBuffers(r))
		MyXactAccessedTempRel = true;

	pgstat_initstats(r);

	return r;
}

/* ----------------
 *		try_relation_open - open any relation by relation OID
 *
 *		Same as relation_open, except return NULL instead of failing
 *		if the relation does not exist.
 * ----------------
 */
Relation
try_relation_open(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* Get the lock first */
	if (lockmode != NoLock)
		LockRelationOid(relationId, lockmode);

	/*
	 * Now that we have the lock, probe to see if the relation really exists
	 * or not.
	 */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relationId)))
	{
		/* Release useless lock */
		if (lockmode != NoLock)
			UnlockRelationOid(relationId, lockmode);

		return NULL;
	}

	/* Should be safe to do a relcache load */
	r = RelationIdGetRelation(relationId);

	if (!RelationIsValid(r))
		elog(ERROR, "could not open relation with OID %u", relationId);

	/* Make note that we've accessed a temporary relation */
	if (RelationUsesLocalBuffers(r))
		MyXactAccessedTempRel = true;

	pgstat_initstats(r);

	return r;
}

/* ----------------
 *		relation_openrv - open any relation specified by a RangeVar
 *
 *		Same as relation_open, but the relation is specified by a RangeVar.
 * ----------------
 */
Relation
relation_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
	Oid			relOid;

	/*
	 * Check for shared-cache-inval messages before trying to open the
	 * relation.  This is needed even if we already hold a lock on the
	 * relation, because GRANT/REVOKE are executed without taking any lock on
	 * the target relation, and we want to be sure we see current ACL
	 * information.  We can skip this if asked for NoLock, on the assumption
	 * that such a call is not the first one in the current command, and so we
	 * should be reasonably up-to-date already.  (XXX this all could stand to
	 * be redesigned, but for the moment we'll keep doing this like it's been
	 * done historically.)
	 */
	if (lockmode != NoLock)
		AcceptInvalidationMessages();

	/* Look up and lock the appropriate relation using namespace search */
	relOid = RangeVarGetRelid(relation, lockmode, false);

	/* Let relation_open do the rest */
	return relation_open(relOid, NoLock);
}

/* ----------------
 *		relation_openrv_extended - open any relation specified by a RangeVar
 *
 *		Same as relation_openrv, but with an additional missing_ok argument
 *		allowing a NULL return rather than an error if the relation is not
 *		found.	(Note that some other causes, such as permissions problems,
 *		will still result in an ereport.)
 * ----------------
 */
Relation
relation_openrv_extended(const RangeVar *relation, LOCKMODE lockmode,
						 bool missing_ok)
{
	Oid			relOid;

	/*
	 * Check for shared-cache-inval messages before trying to open the
	 * relation.  See comments in relation_openrv().
	 */
	if (lockmode != NoLock)
		AcceptInvalidationMessages();

	/* Look up and lock the appropriate relation using namespace search */
	relOid = RangeVarGetRelid(relation, lockmode, missing_ok);

	/* Return NULL on not-found */
	if (!OidIsValid(relOid))
		return NULL;

	/* Let relation_open do the rest */
	return relation_open(relOid, NoLock);
}

/* ----------------
 *		relation_close - close any relation
 *
 *		If lockmode is not "NoLock", we then release the specified lock.
 *
 *		Note that it is often sensible to hold a lock beyond relation_close;
 *		in that case, the lock is released automatically at xact end.
 * ----------------
 */
void
relation_close(Relation relation, LOCKMODE lockmode)
{
	LockRelId	relid = relation->rd_lockInfo.lockRelId;

	Assert(lockmode >= NoLock && lockmode < MAX_LOCKMODES);

	/* The relcache does the real work... */
	RelationClose(relation);

	if (lockmode != NoLock)
		UnlockRelationId(&relid, lockmode);
}


/* ----------------
 *		heap_open - open a heap relation by relation OID
 *
 *		This is essentially relation_open plus check that the relation
 *		is not an index nor a composite type.  (The caller should also
 *		check that it's not a view or foreign table before assuming it has
 *		storage.)
 * ----------------
 */
Relation
heap_open(Oid relationId, LOCKMODE lockmode)
{
	Relation	r;

	r = relation_open(relationId, lockmode);

	if (r->rd_rel->relkind == RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is an index",
						RelationGetRelationName(r))));
	else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a composite type",
						RelationGetRelationName(r))));

	return r;
}

/* ----------------
 *		heap_openrv - open a heap relation specified
 *		by a RangeVar node
 *
 *		As above, but relation is specified by a RangeVar.
 * ----------------
 */
Relation
heap_openrv(const RangeVar *relation, LOCKMODE lockmode)
{
	Relation	r;

	r = relation_openrv(relation, lockmode);

	if (r->rd_rel->relkind == RELKIND_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is an index",
						RelationGetRelationName(r))));
	else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is a composite type",
						RelationGetRelationName(r))));

	return r;
}

/* ----------------
 *		heap_openrv_extended - open a heap relation specified
 *		by a RangeVar node
 *
 *		As above, but optionally return NULL instead of failing for
 *		relation-not-found.
 * ----------------
 */
Relation
heap_openrv_extended(const RangeVar *relation, LOCKMODE lockmode,
					 bool missing_ok)
{
	Relation	r;

	r = relation_openrv_extended(relation, lockmode, missing_ok);

	if (r)
	{
		if (r->rd_rel->relkind == RELKIND_INDEX)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is an index",
							RelationGetRelationName(r))));
		else if (r->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("\"%s\" is a composite type",
							RelationGetRelationName(r))));
	}

	return r;
}


/* ----------------
 *		heap_beginscan	- begin relation scan
 *
 * heap_beginscan_strat offers an extended API that lets the caller control
 * whether a nondefault buffer access strategy can be used, and whether
 * syncscan can be chosen (possibly resulting in the scan not starting from
 * block zero).  Both of these default to TRUE with plain heap_beginscan.
 *
 * heap_beginscan_bm is an alternative entry point for setting up a
 * HeapScanDesc for a bitmap heap scan.  Although that scan technology is
 * really quite unlike a standard seqscan, there is just enough commonality
 * to make it worth using the same data structure.
 * ----------------
 */
HeapScanDesc
heap_beginscan(Relation relation, Snapshot snapshot,
			   int nkeys, ScanKey key)
{
	return heap_beginscan_internal(relation, snapshot, nkeys, key,
								   true, true, false);
}

HeapScanDesc
heap_beginscan_strat(Relation relation, Snapshot snapshot,
					 int nkeys, ScanKey key,
					 bool allow_strat, bool allow_sync)
{
	return heap_beginscan_internal(relation, snapshot, nkeys, key,
								   allow_strat, allow_sync, false);
}

HeapScanDesc
heap_beginscan_bm(Relation relation, Snapshot snapshot,
				  int nkeys, ScanKey key)
{
	return heap_beginscan_internal(relation, snapshot, nkeys, key,
								   false, false, true);
}

static HeapScanDesc
heap_beginscan_internal(Relation relation, Snapshot snapshot,
						int nkeys, ScanKey key,
						bool allow_strat, bool allow_sync,
						bool is_bitmapscan)
{
	HeapScanDesc scan;

	/*
	 * increment relation ref count while scanning relation
	 *
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(relation);

	/*
	 * allocate and initialize scan descriptor
	 */
	scan = (HeapScanDesc) palloc(sizeof(HeapScanDescData));

	scan->rs_rd = relation;
	scan->rs_snapshot = snapshot;
	scan->rs_nkeys = nkeys;
	scan->rs_bitmapscan = is_bitmapscan;
	scan->rs_strategy = NULL;	/* set in initscan */
	scan->rs_allow_strat = allow_strat;
	scan->rs_allow_sync = allow_sync;

	/*
	 * we can use page-at-a-time mode if it's an MVCC-safe snapshot
	 */
	scan->rs_pageatatime = IsMVCCSnapshot(snapshot);

	/*
	 * For a seqscan in a serializable transaction, acquire a predicate lock
	 * on the entire relation. This is required not only to lock all the
	 * matching tuples, but also to conflict with new insertions into the
	 * table. In an indexscan, we take page locks on the index pages covering
	 * the range specified in the scan qual, but in a heap scan there is
	 * nothing more fine-grained to lock. A bitmap scan is a different story,
	 * there we have already scanned the index and locked the index pages
	 * covering the predicate. But in that case we still have to lock any
	 * matching heap tuples.
	 */
	if (!is_bitmapscan)
		PredicateLockRelation(relation, snapshot);

	/* we only need to set this up once */
	scan->rs_ctup.t_tableOid = RelationGetRelid(relation);

	/*
	 * we do this here instead of in initscan() because heap_rescan also calls
	 * initscan() and we don't want to allocate memory again
	 */
	if (nkeys > 0)
		scan->rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->rs_key = NULL;

	initscan(scan, key, false);

	return scan;
}

/* ----------------
 *		heap_rescan		- restart a relation scan
 * ----------------
 */
void
heap_rescan(HeapScanDesc scan,
			ScanKey key)
{
	/*
	 * unpin scan buffers
	 */
	if (BufferIsValid(scan->rs_cbuf))
		ReleaseBuffer(scan->rs_cbuf);

	/*
	 * reinitialize scan descriptor
	 */
	initscan(scan, key, true);
}

/* ----------------
 *		heap_endscan	- end relation scan
 *
 *		See how to integrate with index scans.
 *		Check handling if reldesc caching.
 * ----------------
 */
void
heap_endscan(HeapScanDesc scan)
{
	/* Note: no locking manipulations needed */

	/*
	 * unpin scan buffers
	 */
	if (BufferIsValid(scan->rs_cbuf))
		ReleaseBuffer(scan->rs_cbuf);

	/*
	 * decrement relation reference count and free scan descriptor storage
	 */
	RelationDecrementReferenceCount(scan->rs_rd);

	if (scan->rs_key)
		pfree(scan->rs_key);

	if (scan->rs_strategy != NULL)
		FreeAccessStrategy(scan->rs_strategy);

	pfree(scan);
}

/* ----------------
 *		heap_getnext	- retrieve next tuple in scan
 *
 *		Fix to work with index relations.
 *		We don't return the buffer anymore, but you can get it from the
 *		returned HeapTuple.
 * ----------------
 */

#ifdef HEAPDEBUGALL
#define HEAPDEBUG_1 \
	elog(DEBUG2, "heap_getnext([%s,nkeys=%d],dir=%d) called", \
		 RelationGetRelationName(scan->rs_rd), scan->rs_nkeys, (int) direction)
#define HEAPDEBUG_2 \
	elog(DEBUG2, "heap_getnext returning EOS")
#define HEAPDEBUG_3 \
	elog(DEBUG2, "heap_getnext returning tuple")
#else
#define HEAPDEBUG_1
#define HEAPDEBUG_2
#define HEAPDEBUG_3
#endif   /* !defined(HEAPDEBUGALL) */


HeapTuple
heap_getnext(HeapScanDesc scan, ScanDirection direction)
{
	/* Note: no locking manipulations needed */

	HEAPDEBUG_1;				/* heap_getnext( info ) */

	if (scan->rs_pageatatime)
		heapgettup_pagemode(scan, direction,
							scan->rs_nkeys, scan->rs_key);
	else
		heapgettup(scan, direction, scan->rs_nkeys, scan->rs_key);

	if (scan->rs_ctup.t_data == NULL)
	{
		HEAPDEBUG_2;			/* heap_getnext returning EOS */
		return NULL;
	}

	/*
	 * if we get here it means we have a new current scan tuple, so point to
	 * the proper return buffer and return the tuple.
	 */
	HEAPDEBUG_3;				/* heap_getnext returning tuple */

	pgstat_count_heap_getnext(scan->rs_rd);

	return &(scan->rs_ctup);
}

/*
 *	heap_fetch		- retrieve tuple with given tid
 *
 * On entry, tuple->t_self is the TID to fetch.  We pin the buffer holding
 * the tuple, fill in the remaining fields of *tuple, and check the tuple
 * against the specified snapshot.
 *
 * If successful (tuple found and passes snapshot time qual), then *userbuf
 * is set to the buffer holding the tuple and TRUE is returned.  The caller
 * must unpin the buffer when done with the tuple.
 *
 * If the tuple is not found (ie, item number references a deleted slot),
 * then tuple->t_data is set to NULL and FALSE is returned.
 *
 * If the tuple is found but fails the time qual check, then FALSE is returned
 * but tuple->t_data is left pointing to the tuple.
 *
 * keep_buf determines what is done with the buffer in the FALSE-result cases.
 * When the caller specifies keep_buf = true, we retain the pin on the buffer
 * and return it in *userbuf (so the caller must eventually unpin it); when
 * keep_buf = false, the pin is released and *userbuf is set to InvalidBuffer.
 *
 * stats_relation is the relation to charge the heap_fetch operation against
 * for statistical purposes.  (This could be the heap rel itself, an
 * associated index, or NULL to not count the fetch at all.)
 *
 * heap_fetch does not follow HOT chains: only the exact TID requested will
 * be fetched.
 *
 * It is somewhat inconsistent that we ereport() on invalid block number but
 * return false on invalid item number.  There are a couple of reasons though.
 * One is that the caller can relatively easily check the block number for
 * validity, but cannot check the item number without reading the page
 * himself.  Another is that when we are following a t_ctid link, we can be
 * reasonably confident that the page number is valid (since VACUUM shouldn't
 * truncate off the destination page without having killed the referencing
 * tuple first), but the item number might well not be good.
 */
bool
heap_fetch(Relation relation,
		   Snapshot snapshot,
		   HeapTuple tuple,
		   Buffer *userbuf,
		   bool keep_buf,
		   Relation stats_relation)
{
	ItemPointer tid = &(tuple->t_self);
	ItemId		lp;
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	bool		valid;

	/*
	 * Fetch and pin the appropriate page of the relation.
	 */
	buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));

	/*
	 * Need share lock on buffer to examine tuple commit status.
	 */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);

	/*
	 * We'd better check for out-of-range offnum in case of VACUUM since the
	 * TID was obtained.
	 */
	offnum = ItemPointerGetOffsetNumber(tid);
	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
	{
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		if (keep_buf)
			*userbuf = buffer;
		else
		{
			ReleaseBuffer(buffer);
			*userbuf = InvalidBuffer;
		}
		tuple->t_data = NULL;
		return false;
	}

	/*
	 * get the item line pointer corresponding to the requested tid
	 */
	lp = PageGetItemId(page, offnum);

	/*
	 * Must check for deleted tuple.
	 */
	if (!ItemIdIsNormal(lp))
	{
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		if (keep_buf)
			*userbuf = buffer;
		else
		{
			ReleaseBuffer(buffer);
			*userbuf = InvalidBuffer;
		}
		tuple->t_data = NULL;
		return false;
	}

	/*
	 * fill in *tuple fields
	 */
	tuple->t_data = (HeapTupleStart) PageGetItem(page, lp);
	tuple->t_len = ItemIdGetLength(lp);
	tuple->t_tableOid = RelationGetRelid(relation);

	/*
	 * check time qualification of tuple, then release lock
	 */
	valid = true; // HeapTupleSatisfiesVisibility(tuple, snapshot, buffer);

	if (valid)
		PredicateLockTuple(relation, tuple, snapshot);

	CheckForSerializableConflictOut(valid, relation, tuple, buffer, snapshot);

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	if (valid)
	{
		/*
		 * All checks passed, so return the tuple as valid. Caller is now
		 * responsible for releasing the buffer.
		 */
		*userbuf = buffer;

		/* Count the successful fetch against appropriate rel, if any */
		if (stats_relation != NULL)
			pgstat_count_heap_fetch(stats_relation);

		return true;
	}

	/* Tuple failed time qual, but maybe caller wants to see it anyway. */
	if (keep_buf)
		*userbuf = buffer;
	else
	{
		ReleaseBuffer(buffer);
		*userbuf = InvalidBuffer;
	}

	return false;
}

/*
 *	heap_hot_search_buffer	- search HOT chain for tuple satisfying snapshot
 *
 * On entry, *tid is the TID of a tuple (either a simple tuple, or the root
 * of a HOT chain), and buffer is the buffer holding this tuple.  We search
 * for the first chain member satisfying the given snapshot.  If one is
 * found, we update *tid to reference that tuple's offset number, and
 * return TRUE.  If no match, return FALSE without modifying *tid.
 *
 * heapTuple is a caller-supplied buffer.  When a match is found, we return
 * the tuple here, in addition to updating *tid.  If no match is found, the
 * contents of this buffer on return are undefined.
 *
 * If all_dead is not NULL, we check non-visible tuples to see if they are
 * globally dead; *all_dead is set TRUE if all members of the HOT chain
 * are vacuumable, FALSE if not.
 *
 * Unlike heap_fetch, the caller must already have pin and (at least) share
 * lock on the buffer; it is still pinned/locked at exit.  Also unlike
 * heap_fetch, we do not report any pgstats count; caller may do so if wanted.
 */
bool
heap_hot_search_buffer(ItemPointer tid, Relation relation, Buffer buffer,
					   Snapshot snapshot, HeapTuple heapTuple,
					   bool *all_dead, bool first_call)
{
	Page		dp = (Page) BufferGetPage(buffer);
	TransactionId prev_xmax = InvalidTransactionId;
	OffsetNumber offnum;
	bool		at_chain_start;
	bool		valid;
	bool		skip;

	/* If this is not the first call, previous call returned a (live!) tuple */
	if (all_dead)
		*all_dead = first_call;

	//Assert(TransactionIdIsValid(RecentGlobalXmin));

	Assert(ItemPointerGetBlockNumber(tid) == BufferGetBlockNumber(buffer));
	offnum = ItemPointerGetOffsetNumber(tid);
	at_chain_start = first_call;
	skip = !first_call;

	ItemPointerSetOffsetNumber(tid, offnum);
	PredicateLockTuple(relation, heapTuple, snapshot);
	if (all_dead)
		*all_dead = false;
	return true;

	/* Scan through possible multiple members of HOT-chain */
	// for (;;)
	// {
	// 	ItemId		lp;

	// 	/* check for bogus TID */
	// 	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(dp))
	// 		break;

	// 	lp = PageGetItemId(dp, offnum);

	// 	/* check for unused, dead, or redirected items */
	// 	if (!ItemIdIsNormal(lp))
	// 	{
	// 		/* We should only see a redirect at start of chain */
	// 		if (ItemIdIsRedirected(lp) && at_chain_start)
	// 		{
	// 			/* Follow the redirect */
	// 			offnum = ItemIdGetRedirect(lp);
	// 			at_chain_start = false;
	// 			continue;
	// 		}
	// 		/* else must be end of chain */
	// 		break;
	// 	}

	// 	heapTuple->t_data = (HeapTupleHeader) PageGetItem(dp, lp);
	// 	heapTuple->t_len = ItemIdGetLength(lp);
	// 	heapTuple->t_tableOid = relation->rd_id;
	// 	heapTuple->t_self = *tid;

	// 	/*
	// 	 * Shouldn't see a HEAP_ONLY tuple at chain start.
	// 	 */
	// 	if (at_chain_start && HeapTupleIsHeapOnly(heapTuple))
	// 		break;

	// 	/*
	// 	 * The xmin should match the previous xmax value, else chain is
	// 	 * broken.
	// 	 */
	// 	// if (TransactionIdIsValid(prev_xmax) &&
	// 	// 	!TransactionIdEquals(prev_xmax,
	// 	// 						 HeapTupleHeaderGetXmin(heapTuple->t_data)))
	// 	// 	break;

		
	// 	 * When first_call is true (and thus, skip is initially false) we'll
	// 	 * return the first tuple we find.	But on later passes, heapTuple
	// 	 * will initially be pointing to the tuple we returned last time.
	// 	 * Returning it again would be incorrect (and would loop forever), so
	// 	 * we skip it and return the next match we find.
		 
	// 	if (!skip)
	// 	{
	// 		/* If it's visible per the snapshot, we must return it */
	// 		valid = HeapTupleSatisfiesVisibility(heapTuple, snapshot, buffer);
	// 		CheckForSerializableConflictOut(valid, relation, heapTuple,
	// 										buffer, snapshot);
	// 		if (valid)
	// 		{
	// 			ItemPointerSetOffsetNumber(tid, offnum);
	// 			PredicateLockTuple(relation, heapTuple, snapshot);
	// 			if (all_dead)
	// 				*all_dead = false;
	// 			return true;
	// 		}
	// 	}
	// 	skip = false;

	// 	/*
	// 	 * If we can't see it, maybe no one else can either.  At caller
	// 	 * request, check whether all chain members are dead to all
	// 	 * transactions.
	// 	 */
	// 	if (all_dead && *all_dead &&
	// 		!HeapTupleIsSurelyDead(heapTuple->t_data, RecentGlobalXmin))
	// 		*all_dead = false;

	// 	break;
	// 	/*
	// 	 * Check to see if HOT chain continues past this tuple; if so fetch
	// 	 * the next offnum and loop around.
	// 	 */
	// 	 /*
	// 	if (HeapTupleIsHotUpdated(heapTuple))
	// 	{
	// 		Assert(ItemPointerGetBlockNumber(&heapTuple->t_data->t_ctid) ==
	// 			   ItemPointerGetBlockNumber(tid));
	// 		offnum = ItemPointerGetOffsetNumber(&heapTuple->t_data->t_ctid);
	// 		at_chain_start = false;
	// 		prev_xmax = HeapTupleHeaderGetXmax(heapTuple->t_data);
	// 	}
	// 	else
	// 		break;	
	// 	*/			/* end of chain */
	// }

	// return false;
}

/*
 *	heap_hot_search		- search HOT chain for tuple satisfying snapshot
 *
 * This has the same API as heap_hot_search_buffer, except that the caller
 * does not provide the buffer containing the page, rather we access it
 * locally.
 */
bool
heap_hot_search(ItemPointer tid, Relation relation, Snapshot snapshot,
				bool *all_dead)
{
	bool		result;
	Buffer		buffer;
	HeapTupleData heapTuple;

	buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	result = heap_hot_search_buffer(tid, relation, buffer, snapshot,
									&heapTuple, all_dead, true);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
	return result;
}

/*
 *	heap_get_latest_tid -  get the latest tid of a specified tuple
 *
 * Actually, this gets the latest version that is visible according to
 * the passed snapshot.  You can pass SnapshotDirty to get the very latest,
 * possibly uncommitted version.
 *
 * *tid is both an input and an output parameter: it is updated to
 * show the latest version of the row.	Note that it will not be changed
 * if no version of the row passes the snapshot test.
 */
void
heap_get_latest_tid(Relation relation,
					Snapshot snapshot,
					ItemPointer tid)
{
	BlockNumber blk;
	ItemPointerData ctid;
	TransactionId priorXmax;

	/* this is to avoid Assert failures on bad input */
	if (!ItemPointerIsValid(tid))
		return;

	/*
	 * Since this can be called with user-supplied TID, don't trust the input
	 * too much.  (RelationGetNumberOfBlocks is an expensive check, so we
	 * don't check t_ctid links again this way.  Note that it would not do to
	 * call it just once and save the result, either.)
	 */
	blk = ItemPointerGetBlockNumber(tid);
	if (blk >= RelationGetNumberOfBlocks(relation))
		elog(ERROR, "block number %u is out of range for relation \"%s\"",
			 blk, RelationGetRelationName(relation));

	/*
	 * Loop to chase down t_ctid links.  At top of loop, ctid is the tuple we
	 * need to examine, and *tid is the TID we will return if ctid turns out
	 * to be bogus.
	 *
	 * Note that we will loop until we reach the end of the t_ctid chain.
	 * Depending on the snapshot passed, there might be at most one visible
	 * version of the row, but we don't try to optimize for that.
	 */
	ctid = *tid;
	Buffer buffer;
	Page page;
	OffsetNumber offnum;
	ItemId lp;
	HeapTupleData tp;
	bool valid;

	tp.t_self = ctid;
	tp.t_data = (HeapTupleHeader) PageGetItem(page, lp);
	tp.t_len = ItemIdGetLength(lp);
	*tid = ctid;
	UnlockReleaseBuffer(buffer);


	// priorXmax = InvalidTransactionId;	/* cannot check first XMIN */
	// for (;;)
	// {
	// 	Buffer		buffer;
	// 	Page		page;
	// 	OffsetNumber offnum;
	// 	ItemId		lp;
	// 	HeapTupleData tp;
	// 	bool		valid;

	// 	/*
	// 	 * Read, pin, and lock the page.
	// 	 */
	// 	buffer = ReadBuffer(relation, ItemPointerGetBlockNumber(&ctid));
	// 	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	// 	page = BufferGetPage(buffer);

	// 	/*
	// 	 * Check for bogus item number.  This is not treated as an error
	// 	 * condition because it can happen while following a t_ctid link. We
	// 	 * just assume that the prior tid is OK and return it unchanged.
	// 	 */
	// 	offnum = ItemPointerGetOffsetNumber(&ctid);
	// 	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
	// 	{
	// 		UnlockReleaseBuffer(buffer);
	// 		break;
	// 	}
	// 	lp = PageGetItemId(page, offnum);
	// 	if (!ItemIdIsNormal(lp))
	// 	{
	// 		UnlockReleaseBuffer(buffer);
	// 		break;
	// 	}

	// 	/* OK to access the tuple */
	// 	tp.t_self = ctid;
	// 	tp.t_data = (HeapTupleHeader) PageGetItem(page, lp);
	// 	tp.t_len = ItemIdGetLength(lp);

	// 	/*
	// 	 * After following a t_ctid link, we might arrive at an unrelated
	// 	 * tuple.  Check for XMIN match.
	// 	 */
	// 	// if (TransactionIdIsValid(priorXmax) &&
	// 	//   !TransactionIdEquals(priorXmax, HeapTupleHeaderGetXmin(tp.t_data)))
	// 	// {
	// 	// 	UnlockReleaseBuffer(buffer);
	// 	// 	break;
	// 	// }

	// 	/*
	// 	 * Check time qualification of tuple; if visible, set it as the new
	// 	 * result candidate.
	// 	 */
	// 	// valid = HeapTupleSatisfiesVisibility(&tp, snapshot, buffer);
	// 	// CheckForSerializableConflictOut(valid, relation, &tp, buffer, snapshot);
	// 	// if (valid)
	// 		*tid = ctid;

	// 	/*
	// 	 * If there's a valid t_ctid link, follow it, else we're done.
	// 	 */
	// 	// if ((tp.t_data->t_infomask & (HEAP_XMAX_INVALID | HEAP_IS_LOCKED)) ||
	// 	// 	ItemPointerEquals(&tp.t_self, &tp.t_data->t_ctid))
	// 	// {
	// 		UnlockReleaseBuffer(buffer);
	// 		break;
	// 	//}

	// 	// ctid = tp.t_data->t_ctid;
	// 	// priorXmax = HeapTupleHeaderGetXmax(tp.t_data);
	// 	// UnlockReleaseBuffer(buffer);
	// }							/* end of loop */
}


/*
 * UpdateXmaxHintBits - update tuple hint bits after xmax transaction ends
 *
 * This is called after we have waited for the XMAX transaction to terminate.
 * If the transaction aborted, we guarantee the XMAX_INVALID hint bit will
 * be set on exit.	If the transaction committed, we set the XMAX_COMMITTED
 * hint bit if possible --- but beware that that may not yet be possible,
 * if the transaction committed asynchronously.  Hence callers should look
 * only at XMAX_INVALID.
 */
static void
UpdateXmaxHintBits(HeapTupleHeader tuple, Buffer buffer, TransactionId xid)
{
	// Assert(TransactionIdEquals(HeapTupleHeaderGetXmax(tuple), xid));

	// if (!(tuple->t_infomask & (HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID)))
	// {
	// 	if (TransactionIdDidCommit(xid))
	// 		HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_COMMITTED,
	// 							 xid);
	// 	else
	// 		HeapTupleSetHintBits(tuple, buffer, HEAP_XMAX_INVALID,
	// 							 InvalidTransactionId);
	// }
}


/*
 * GetBulkInsertState - prepare status object for a bulk insert
 */
BulkInsertState
GetBulkInsertState(void)
{
	BulkInsertState bistate;

	bistate = (BulkInsertState) palloc(sizeof(BulkInsertStateData));
	bistate->strategy = GetAccessStrategy(BAS_BULKWRITE);
	bistate->current_buf = InvalidBuffer;
	return bistate;
}

/*
 * FreeBulkInsertState - clean up after finishing a bulk insert
 */
void
FreeBulkInsertState(BulkInsertState bistate)
{
	if (bistate->current_buf != InvalidBuffer)
		ReleaseBuffer(bistate->current_buf);
	FreeAccessStrategy(bistate->strategy);
	pfree(bistate);
}


/*
 *	heap_insert		- insert tuple into a heap
 *
 * The new tuple is stamped with current transaction ID and the specified
 * command ID.
 *
 * If the HEAP_INSERT_SKIP_WAL option is specified, the new tuple is not
 * logged in WAL, even for a non-temp relation.  Safe usage of this behavior
 * requires that we arrange that all new tuples go into new pages not
 * containing any tuples from other transactions, and that the relation gets
 * fsync'd before commit.  (See also heap_sync() comments)
 *
 * The HEAP_INSERT_SKIP_FSM option is passed directly to
 * RelationGetBufferForTuple, which see for more info.
 *
 * Note that these options will be applied when inserting into the heap's
 * TOAST table, too, if the tuple requires any out-of-line data.
 *
 * The BulkInsertState object (if any; bistate can be NULL for default
 * behavior) is also just passed through to RelationGetBufferForTuple.
 *
 * The return value is the OID assigned to the tuple (either here or by the
 * caller), or InvalidOid if no OID.  The header fields of *tup are updated
 * to match the stored tuple; in particular tup->t_self receives the actual
 * TID where the tuple was stored.	But note that any toasting of fields
 * within the tuple data is NOT reflected into *tup.
 */
Oid
heap_insert(Relation relation, HeapTuple tup, CommandId cid,
			int options, BulkInsertState bistate)
{
	TransactionId xid = GetCurrentTransactionId();
	HeapTuple	heaptup;
	Buffer		buffer;
	Buffer		vmbuffer = InvalidBuffer;
	bool		all_visible_cleared = false;

	elog(DEBUG4, "Heap inserting");

	/*
	 * Fill in tuple header fields, assign an OID, and toast the tuple if
	 * necessary.
	 *
	 * Note: below this point, heaptup is the data we actually intend to store
	 * into the relation; tup is the caller's original untoasted data.
	 */
	heaptup = heap_prepare_insert(relation, tup, xid, cid, options);

	/*
	 * We're about to do the actual insert -- but check for conflict first, to
	 * avoid possibly having to roll back work we've just done.
	 *
	 * For a heap insert, we only need to check for table-level SSI locks. Our
	 * new tuple can't possibly conflict with existing tuple locks, and heap
	 * page locks are only consolidated versions of tuple locks; they do not
	 * lock "gaps" as index page locks do.	So we don't need to identify a
	 * buffer before making the call.
	 */
	// CheckForSerializableConflictIn(relation, NULL, InvalidBuffer);

	/*
	 * Find buffer to insert this tuple into.  If the page is all visible,
	 * this will also pin the requisite visibility map page.
	 */
	buffer = RelationGetBufferForTuple(relation, heaptup->t_len,
									   InvalidBuffer, options, bistate,
									   &vmbuffer, NULL);

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	RelationPutHeapTuple(relation, buffer, heaptup);

	if (PageIsAllVisible(BufferGetPage(buffer)))
	{
		all_visible_cleared = true;
		PageClearAllVisible(BufferGetPage(buffer));
		visibilitymap_clear(relation,
							ItemPointerGetBlockNumber(&(heaptup->t_self)),
							vmbuffer);
	}

	/*
	 * XXX Should we set PageSetPrunable on this page ?
	 *
	 * The inserting transaction may eventually abort thus making this tuple
	 * DEAD and hence available for pruning. Though we don't want to optimize
	 * for aborts, if no other tuple in this page is UPDATEd/DELETEd, the
	 * aborted tuple will never be pruned until next vacuum is triggered.
	 *
	 * If you do add PageSetPrunable here, add it in heap_xlog_insert too.
	 */

	// MarkBufferDirty(buffer);

	/* XLOG stuff */
	if (false && !(options & HEAP_INSERT_SKIP_WAL) && RelationNeedsWAL(relation))
	{
		xl_heap_insert xlrec;
		xl_heap_header xlhdr;
		XLogRecPtr	recptr;
		XLogRecData rdata[3];
		Page		page = BufferGetPage(buffer);
		uint8		info = XLOG_HEAP_INSERT;

		xlrec.all_visible_cleared = all_visible_cleared;
		xlrec.target.node = relation->rd_node;
		xlrec.target.tid = heaptup->t_self;
		rdata[0].data = (char *) &xlrec;
		rdata[0].len = SizeOfHeapInsert;
		rdata[0].buffer = InvalidBuffer;
		rdata[0].next = &(rdata[1]);

		// xlhdr.t_infomask2 = heaptup->t_data->t_infomask2;
		// xlhdr.t_infomask = heaptup->t_data->t_infomask;
		// xlhdr.t_hoff = heaptup->t_data->t_hoff;

		/*
		 * note we mark rdata[1] as belonging to buffer; if XLogInsert decides
		 * to write the whole page to the xlog, we don't need to store
		 * xl_heap_header in the xlog.
		 */
		rdata[1].data = (char *) &xlhdr;
		rdata[1].len = SizeOfHeapHeader;
		rdata[1].buffer = buffer;
		rdata[1].buffer_std = true;
		rdata[1].next = &(rdata[2]);

		/* PG73FORMAT: write bitmap [+ padding] [+ oid] + data */
		rdata[2].data = (char *) heaptup->t_data + offsetof(HeapTupleHeaderData, t_bits);
		rdata[2].len = heaptup->t_len - offsetof(HeapTupleHeaderData, t_bits);
		rdata[2].buffer = buffer;
		rdata[2].buffer_std = true;
		rdata[2].next = NULL;

		/*
		 * If this is the single and first tuple on page, we can reinit the
		 * page instead of restoring the whole thing.  Set flag, and hide
		 * buffer references from XLogInsert.
		 */
		if (ItemPointerGetOffsetNumber(&(heaptup->t_self)) == FirstOffsetNumber &&
			PageGetMaxOffsetNumber(page) == FirstOffsetNumber)
		{
			info |= XLOG_HEAP_INIT_PAGE;
			rdata[1].buffer = rdata[2].buffer = InvalidBuffer;
		}

		recptr = XLogInsert(RM_HEAP_ID, info, rdata);

		PageSetLSN(page, recptr);
		PageSetTLI(page, ThisTimeLineID);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buffer);
	if (vmbuffer != InvalidBuffer)
		ReleaseBuffer(vmbuffer);

	/*
	 * If tuple is cachable, mark it for invalidation from the caches in case
	 * we abort.  Note it is OK to do this after releasing the buffer, because
	 * the heaptup data structure is all in local memory, not in the shared
	 * buffer.
	 */
	CacheInvalidateHeapTuple(relation, heaptup, NULL);

	pgstat_count_heap_insert(relation, 1);

	/*
	 * If heaptup is a private copy, release it.  Don't forget to copy t_self
	 * back to the caller's image, too.
	 */
	if (heaptup != tup)
	{
		tup->t_self = heaptup->t_self;
		heap_freetuple(heaptup);
	}

	return tup->t_tableOid;
}

/*
 * Subroutine for heap_insert(). Prepares a tuple for insertion. This sets the
 * tuple header fields, assigns an OID, and toasts the tuple if necessary.
 * Returns a toasted version of the tuple if it was toasted, or the original
 * tuple if not. Note that in any case, the header fields are also set in
 * the original tuple.
 */
static HeapTuple
heap_prepare_insert(Relation relation, HeapTuple tup, TransactionId xid,
					CommandId cid, int options)
{
	tup->t_tableOid = RelationGetRelid(relation);
	return tup;
}

/*
 *	heap_multi_insert	- insert multiple tuple into a heap
 *
 * This is like heap_insert(), but inserts multiple tuples in one operation.
 * That's faster than calling heap_insert() in a loop, because when multiple
 * tuples can be inserted on a single page, we can write just a single WAL
 * record covering all of them, and only need to lock/unlock the page once.
 *
 * Note: this leaks memory into the current memory context. You can create a
 * temporary context before calling this, if that's a problem.
 */
void
heap_multi_insert(Relation relation, HeapTuple *tuples, int ntuples,
				  CommandId cid, int options, BulkInsertState bistate)
{
	TransactionId xid = GetCurrentTransactionId();
	HeapTuple  *heaptuples;
	int			i;
	int			ndone;
	char	   *scratch = NULL;
	Page		page;
	bool		needwal;
	Size		saveFreeSpace;

	needwal = !(options & HEAP_INSERT_SKIP_WAL) && RelationNeedsWAL(relation);
	saveFreeSpace = RelationGetTargetPageFreeSpace(relation,
												   HEAP_DEFAULT_FILLFACTOR);

	/* Toast and set header data in all the tuples */
	heaptuples = palloc(ntuples * sizeof(HeapTuple));
	for (i = 0; i < ntuples; i++)
		heaptuples[i] = heap_prepare_insert(relation, tuples[i],
											xid, cid, options);

	/*
	 * Allocate some memory to use for constructing the WAL record. Using
	 * palloc() within a critical section is not safe, so we allocate this
	 * beforehand.
	 */
	if (needwal)
		scratch = palloc(BLCKSZ);

	/*
	 * We're about to do the actual inserts -- but check for conflict first,
	 * to avoid possibly having to roll back work we've just done.
	 *
	 * For a heap insert, we only need to check for table-level SSI locks. Our
	 * new tuple can't possibly conflict with existing tuple locks, and heap
	 * page locks are only consolidated versions of tuple locks; they do not
	 * lock "gaps" as index page locks do.	So we don't need to identify a
	 * buffer before making the call.
	 */
	// CheckForSerializableConflictIn(relation, NULL, InvalidBuffer);

	ndone = 0;
	while (ndone < ntuples)
	{
		Buffer		buffer;
		Buffer		vmbuffer = InvalidBuffer;
		bool		all_visible_cleared = false;
		int			nthispage;

		/*
		 * Find buffer where at least the next tuple will fit.	If the page is
		 * all-visible, this will also pin the requisite visibility map page.
		 */
		buffer = RelationGetBufferForTuple(relation, heaptuples[ndone]->t_len,
										   InvalidBuffer, options, bistate,
										   &vmbuffer, NULL);
		page = BufferGetPage(buffer);

		/* NO EREPORT(ERROR) from here till changes are logged */
		START_CRIT_SECTION();

		/* Put as many tuples as fit on this page */
		for (nthispage = 0; ndone + nthispage < ntuples; nthispage++)
		{
			HeapTuple	heaptup = heaptuples[ndone + nthispage];

			if (PageGetHeapFreeSpace(page) < MAXALIGN(TUPLESIZE) + saveFreeSpace)
				break;

			RelationPutHeapTuple(relation, buffer, heaptup);
		}

		/*
		 * XXX Should we set PageSetPrunable on this page ? See heap_insert()
		 */

		MarkBufferDirty(buffer);

		/* XLOG stuff */
		if (needwal)
		{
			XLogRecPtr	recptr;
			xl_heap_multi_insert *xlrec;
			XLogRecData rdata[2];
			uint8		info = XLOG_HEAP2_MULTI_INSERT;
			char	   *tupledata;
			int			totaldatalen;
			char	   *scratchptr = scratch;
			bool		init;

			/*
			 * If the page was previously empty, we can reinit the page
			 * instead of restoring the whole thing.
			 */
			init = (ItemPointerGetOffsetNumber(&(heaptuples[ndone]->t_self)) == FirstOffsetNumber &&
					PageGetMaxOffsetNumber(page) == FirstOffsetNumber + nthispage - 1);

			/* allocate xl_heap_multi_insert struct from the scratch area */
			xlrec = (xl_heap_multi_insert *) scratchptr;
			scratchptr += SizeOfHeapMultiInsert;

			/*
			 * Allocate offsets array. Unless we're reinitializing the page,
			 * in that case the tuples are stored in order starting at
			 * FirstOffsetNumber and we don't need to store the offsets
			 * explicitly.
			 */
			if (!init)
				scratchptr += nthispage * sizeof(OffsetNumber);

			/* the rest of the scratch space is used for tuple data */
			tupledata = scratchptr;

			xlrec->all_visible_cleared = all_visible_cleared;
			xlrec->node = relation->rd_node;
			xlrec->blkno = BufferGetBlockNumber(buffer);
			xlrec->ntuples = nthispage;

			/*
			 * Write out an xl_multi_insert_tuple and the tuple data itself
			 * for each tuple.
			 */
			for (i = 0; i < nthispage; i++)
			{
				HeapTuple	heaptup = heaptuples[ndone + i];
				xl_multi_insert_tuple *tuphdr;
				int			datalen;

				if (!init)
					xlrec->offsets[i] = ItemPointerGetOffsetNumber(&heaptup->t_self);
				/* xl_multi_insert_tuple needs two-byte alignment. */
				tuphdr = (xl_multi_insert_tuple *) SHORTALIGN(scratchptr);
				scratchptr = ((char *) tuphdr) + SizeOfMultiInsertTuple;

				// tuphdr->t_infomask2 = heaptup->t_data->t_infomask2;
				// tuphdr->t_infomask = heaptup->t_data->t_infomask;
				// tuphdr->t_hoff = heaptup->t_data->t_hoff;

				/* write bitmap [+ padding] [+ oid] + data */
				datalen = TUPLESIZE;
				memcpy(scratchptr,
					   (char *) heaptup->t_data,
					   datalen);
				scratchptr += TUPLESIZE;
			}
			totaldatalen = scratchptr - tupledata;
			Assert((scratchptr - scratch) < BLCKSZ);

			rdata[0].data = (char *) xlrec;
			rdata[0].len = tupledata - scratch;
			rdata[0].buffer = InvalidBuffer;
			rdata[0].next = &rdata[1];

			rdata[1].data = tupledata;
			rdata[1].len = totaldatalen;
			rdata[1].buffer = buffer;
			rdata[1].buffer_std = true;
			rdata[1].next = NULL;

			/*
			 * If we're going to reinitialize the whole page using the WAL
			 * record, hide buffer reference from XLogInsert.
			 */
			if (init)
			{
				rdata[1].buffer = InvalidBuffer;
				info |= XLOG_HEAP_INIT_PAGE;
			}

			recptr = XLogInsert(RM_HEAP2_ID, info, rdata);

			PageSetLSN(page, recptr);
			PageSetTLI(page, ThisTimeLineID);
		}

		END_CRIT_SECTION();

		UnlockReleaseBuffer(buffer);
		if (vmbuffer != InvalidBuffer)
			ReleaseBuffer(vmbuffer);

		ndone += nthispage;
	}

	/*
	 * If tuples are cachable, mark them for invalidation from the caches in
	 * case we abort.  Note it is OK to do this after releasing the buffer,
	 * because the heaptuples data structure is all in local memory, not in
	 * the shared buffer.
	 */
	if (IsSystemRelation(relation))
	{
		for (i = 0; i < ntuples; i++)
			CacheInvalidateHeapTuple(relation, heaptuples[i], NULL);
	}

	/*
	 * Copy t_self fields back to the caller's original tuples. This does
	 * nothing for untoasted tuples (tuples[i] == heaptuples[i)], but it's
	 * probably faster to always copy than check.
	 */
	for (i = 0; i < ntuples; i++)
		tuples[i]->t_self = heaptuples[i]->t_self;

	pgstat_count_heap_insert(relation, ntuples);
}

/*
 *	simple_heap_insert - insert a tuple
 *
 * Currently, this routine differs from heap_insert only in supplying
 * a default command ID and not allowing access to the speedup options.
 *
 * This should be used rather than using heap_insert directly in most places
 * where we are modifying system catalogs.
 */
Oid
simple_heap_insert(Relation relation, HeapTuple tup)
{
	elog(DEBUG4, "Simple heap insert");
	return heap_insert(relation, tup, GetCurrentCommandId(true), 0, NULL);
}

/*
 *	heap_delete - delete a tuple
 *
 * NB: do not call this directly unless you are prepared to deal with
 * concurrent-update conditions.  Use simple_heap_delete instead.
 *
 *	relation - table to be modified (caller must hold suitable lock)
 *	tid - TID of tuple to be deleted
 *	ctid - output parameter, used only for failure case (see below)
 *	update_xmax - output parameter, used only for failure case (see below)
 *	cid - delete command ID (used for visibility test, and stored into
 *		cmax if successful)
 *	crosscheck - if not InvalidSnapshot, also check tuple against this
 *	wait - true if should wait for any conflicting update to commit/abort
 *
 * Normal, successful return value is HeapTupleMayBeUpdated, which
 * actually means we did delete it.  Failure return codes are
 * HeapTupleSelfUpdated, HeapTupleUpdated, or HeapTupleBeingUpdated
 * (the last only possible if wait == false).
 *
 * In the failure cases, the routine returns the tuple's t_ctid and t_xmax.
 * If t_ctid is the same as tid, the tuple was deleted; if different, the
 * tuple was updated, and t_ctid is the location of the replacement tuple.
 * (t_xmax is needed to verify that the replacement tuple matches.)
 */
HTSU_Result
heap_delete(Relation relation, ItemPointer tid,
			ItemPointer ctid, TransactionId *update_xmax,
			CommandId cid, Snapshot crosscheck, bool wait)
{

	return HeapTupleMayBeUpdated;
}

/*
 *	simple_heap_delete - delete a tuple
 *
 * This routine may be used to delete a tuple when concurrent updates of
 * the target tuple are not expected (for example, because we have a lock
 * on the relation associated with the tuple).	Any failure is reported
 * via ereport().
 */
void
simple_heap_delete(Relation relation, ItemPointer tid)
{
	HTSU_Result result;
	ItemPointerData update_ctid;
	TransactionId update_xmax;

	result = heap_delete(relation, tid,
						 &update_ctid, &update_xmax,
						 GetCurrentCommandId(true), InvalidSnapshot,
						 true /* wait for commit */ );
	switch (result)
	{
		case HeapTupleSelfUpdated:
			/* Tuple was already updated in current command? */
			elog(ERROR, "tuple already updated by self");
			break;

		case HeapTupleMayBeUpdated:
			/* done successfully */
			break;

		case HeapTupleUpdated:
			elog(ERROR, "tuple concurrently updated");
			break;

		default:
			elog(ERROR, "unrecognized heap_delete status: %u", result);
			break;
	}
}

/*
 *	heap_update - replace a tuple
 *
 * NB: do not call this directly unless you are prepared to deal with
 * concurrent-update conditions.  Use simple_heap_update instead.
 *
 *	relation - table to be modified (caller must hold suitable lock)
 *	otid - TID of old tuple to be replaced
 *	newtup - newly constructed tuple data to store
 *	ctid - output parameter, used only for failure case (see below)
 *	update_xmax - output parameter, used only for failure case (see below)
 *	cid - update command ID (used for visibility test, and stored into
 *		cmax/cmin if successful)
 *	crosscheck - if not InvalidSnapshot, also check old tuple against this
 *	wait - true if should wait for any conflicting update to commit/abort
 *
 * Normal, successful return value is HeapTupleMayBeUpdated, which
 * actually means we *did* update it.  Failure return codes are
 * HeapTupleSelfUpdated, HeapTupleUpdated, or HeapTupleBeingUpdated
 * (the last only possible if wait == false).
 *
 * On success, the header fields of *newtup are updated to match the new
 * stored tuple; in particular, newtup->t_self is set to the TID where the
 * new tuple was inserted, and its HEAP_ONLY_TUPLE flag is set iff a HOT
 * update was done.  However, any TOAST changes in the new tuple's
 * data are not reflected into *newtup.
 *
 * In the failure cases, the routine returns the tuple's t_ctid and t_xmax.
 * If t_ctid is the same as otid, the tuple was deleted; if different, the
 * tuple was updated, and t_ctid is the location of the replacement tuple.
 * (t_xmax is needed to verify that the replacement tuple matches.)
 */
HTSU_Result
heap_update(Relation relation, ItemPointer otid, HeapTuple newtup,
			ItemPointer ctid, TransactionId *update_xmax,
			CommandId cid, Snapshot crosscheck, bool wait)
{
	return HeapTupleMayBeUpdated;
}

/*
 * Check if the specified attribute's value is same in both given tuples.
 * Subroutine for HeapSatisfiesHOTUpdate.
 */
static bool
heap_tuple_attr_equals(TupleDesc tupdesc, int attrnum,
					   HeapTuple tup1, HeapTuple tup2)
{
	Datum		value1,
				value2;
	bool		isnull1,
				isnull2;
	Form_pg_attribute att;

	/*
	 * If it's a whole-tuple reference, say "not equal".  It's not really
	 * worth supporting this case, since it could only succeed after a no-op
	 * update, which is hardly a case worth optimizing for.
	 */
	if (attrnum == 0)
		return false;

	/*
	 * Likewise, automatically say "not equal" for any system attribute other
	 * than OID and tableOID; we cannot expect these to be consistent in a HOT
	 * chain, or even to be set correctly yet in the new tuple.
	 */
	if (attrnum < 0)
	{
		if (attrnum != ObjectIdAttributeNumber &&
			attrnum != TableOidAttributeNumber)
			return false;
	}

	/*
	 * Extract the corresponding values.  XXX this is pretty inefficient if
	 * there are many indexed columns.	Should HeapSatisfiesHOTUpdate do a
	 * single heap_deform_tuple call on each tuple, instead?  But that doesn't
	 * work for system columns ...
	 */
	value1 = heap_getattr(tup1, attrnum, tupdesc, &isnull1);
	value2 = heap_getattr(tup2, attrnum, tupdesc, &isnull2);

	/*
	 * If one value is NULL and other is not, then they are certainly not
	 * equal
	 */
	if (isnull1 != isnull2)
		return false;

	/*
	 * If both are NULL, they can be considered equal.
	 */
	if (isnull1)
		return true;

	/*
	 * We do simple binary comparison of the two datums.  This may be overly
	 * strict because there can be multiple binary representations for the
	 * same logical value.	But we should be OK as long as there are no false
	 * positives.  Using a type-specific equality operator is messy because
	 * there could be multiple notions of equality in different operator
	 * classes; furthermore, we cannot safely invoke user-defined functions
	 * while holding exclusive buffer lock.
	 */
	if (attrnum <= 0)
	{
		/* The only allowed system columns are OIDs, so do this */
		return (DatumGetObjectId(value1) == DatumGetObjectId(value2));
	}
	else
	{
		Assert(attrnum <= tupdesc->natts);
		att = tupdesc->attrs[attrnum - 1];
		return datumIsEqual(value1, value2, att->attbyval, att->attlen);
	}
}

/*
 * Check if the old and new tuples represent a HOT-safe update. To be able
 * to do a HOT update, we must not have changed any columns used in index
 * definitions.
 *
 * The set of attributes to be checked is passed in (we dare not try to
 * compute it while holding exclusive buffer lock...)  NOTE that hot_attrs
 * is destructively modified!  That is OK since this is invoked at most once
 * by heap_update().
 *
 * Returns true if safe to do HOT update.
 */
static bool
HeapSatisfiesHOTUpdate(Relation relation, Bitmapset *hot_attrs,
					   HeapTuple oldtup, HeapTuple newtup)
{
	int			attrnum;

	while ((attrnum = bms_first_member(hot_attrs)) >= 0)
	{
		/* Adjust for system attributes */
		attrnum += FirstLowInvalidHeapAttributeNumber;

		/* If the attribute value has changed, we can't do HOT update */
		if (!heap_tuple_attr_equals(RelationGetDescr(relation), attrnum,
									oldtup, newtup))
			return false;
	}

	return true;
}

/*
 *	simple_heap_update - replace a tuple
 *
 * This routine may be used to update a tuple when concurrent updates of
 * the target tuple are not expected (for example, because we have a lock
 * on the relation associated with the tuple).	Any failure is reported
 * via ereport().
 */
void
simple_heap_update(Relation relation, ItemPointer otid, HeapTuple tup)
{
	HTSU_Result result;
	ItemPointerData update_ctid;
	TransactionId update_xmax;

	result = heap_update(relation, otid, tup,
						 &update_ctid, &update_xmax,
						 GetCurrentCommandId(true), InvalidSnapshot,
						 true /* wait for commit */ );
	switch (result)
	{
		case HeapTupleSelfUpdated:
			/* Tuple was already updated in current command? */
			elog(ERROR, "tuple already updated by self");
			break;

		case HeapTupleMayBeUpdated:
			/* done successfully */
			break;

		case HeapTupleUpdated:
			elog(ERROR, "tuple concurrently updated");
			break;

		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
			break;
	}
}

/*
 *	heap_lock_tuple - lock a tuple in shared or exclusive mode
 *
 * Note that this acquires a buffer pin, which the caller must release.
 *
 * Input parameters:
 *	relation: relation containing tuple (caller must hold suitable lock)
 *	tuple->t_self: TID of tuple to lock (rest of struct need not be valid)
 *	cid: current command ID (used for visibility test, and stored into
 *		tuple's cmax if lock is successful)
 *	mode: indicates if shared or exclusive tuple lock is desired
 *	nowait: if true, ereport rather than blocking if lock not available
 *
 * Output parameters:
 *	*tuple: all fields filled in
 *	*buffer: set to buffer holding tuple (pinned but not locked at exit)
 *	*ctid: set to tuple's t_ctid, but only in failure cases
 *	*update_xmax: set to tuple's xmax, but only in failure cases
 *
 * Function result may be:
 *	HeapTupleMayBeUpdated: lock was successfully acquired
 *	HeapTupleSelfUpdated: lock failed because tuple updated by self
 *	HeapTupleUpdated: lock failed because tuple updated by other xact
 *
 * In the failure cases, the routine returns the tuple's t_ctid and t_xmax.
 * If t_ctid is the same as t_self, the tuple was deleted; if different, the
 * tuple was updated, and t_ctid is the location of the replacement tuple.
 * (t_xmax is needed to verify that the replacement tuple matches.)
 *
 *
 * NOTES: because the shared-memory lock table is of finite size, but users
 * could reasonably want to lock large numbers of tuples, we do not rely on
 * the standard lock manager to store tuple-level locks over the long term.
 * Instead, a tuple is marked as locked by setting the current transaction's
 * XID as its XMAX, and setting additional infomask bits to distinguish this
 * usage from the more normal case of having deleted the tuple.  When
 * multiple transactions concurrently share-lock a tuple, the first locker's
 * XID is replaced in XMAX with a MultiTransactionId representing the set of
 * XIDs currently holding share-locks.
 *
 * When it is necessary to wait for a tuple-level lock to be released, the
 * basic delay is provided by XactLockTableWait or MultiXactIdWait on the
 * contents of the tuple's XMAX.  However, that mechanism will release all
 * waiters concurrently, so there would be a race condition as to which
 * waiter gets the tuple, potentially leading to indefinite starvation of
 * some waiters.  The possibility of share-locking makes the problem much
 * worse --- a steady stream of share-lockers can easily block an exclusive
 * locker forever.	To provide more reliable semantics about who gets a
 * tuple-level lock first, we use the standard lock manager.  The protocol
 * for waiting for a tuple-level lock is really
 *		LockTuple()
 *		XactLockTableWait()
 *		mark tuple as locked by me
 *		UnlockTuple()
 * When there are multiple waiters, arbitration of who is to get the lock next
 * is provided by LockTuple().	However, at most one tuple-level lock will
 * be held or awaited per backend at any time, so we don't risk overflow
 * of the lock table.  Note that incoming share-lockers are required to
 * do LockTuple as well, if there is any conflict, to ensure that they don't
 * starve out waiting exclusive-lockers.  However, if there is not any active
 * conflict for a tuple, we don't incur any extra overhead.
 */
HTSU_Result
heap_lock_tuple(Relation relation, HeapTuple tuple, Buffer *buffer,
				ItemPointer ctid, TransactionId *update_xmax,
				CommandId cid, LockTupleMode mode, bool nowait)
{
	return HeapTupleMayBeUpdated;
}


/*
 * heap_inplace_update - update a tuple "in place" (ie, overwrite it)
 *
 * Overwriting violates both MVCC and transactional safety, so the uses
 * of this function in Postgres are extremely limited.	Nonetheless we
 * find some places to use it.
 *
 * The tuple cannot change size, and therefore it's reasonable to assume
 * that its null bitmap (if any) doesn't change either.  So we just
 * overwrite the data portion of the tuple without touching the null
 * bitmap or any of the header fields.
 *
 * tuple is an in-memory tuple structure containing the data to be written
 * over the target tuple.  Also, tuple->t_self identifies the target tuple.
 */
void
heap_inplace_update(Relation relation, HeapTuple tuple)
{
}


/*
 * heap_freeze_tuple
 *
 * Check to see whether any of the XID fields of a tuple (xmin, xmax, xvac)
 * are older than the specified cutoff XID.  If so, replace them with
 * FrozenTransactionId or InvalidTransactionId as appropriate, and return
 * TRUE.  Return FALSE if nothing was changed.
 *
 * It is assumed that the caller has checked the tuple with
 * HeapTupleSatisfiesVacuum() and determined that it is not HEAPTUPLE_DEAD
 * (else we should be removing the tuple, not freezing it).
 *
 * NB: cutoff_xid *must* be <= the current global xmin, to ensure that any
 * XID older than it could neither be running nor seen as running by any
 * open transaction.  This ensures that the replacement will not change
 * anyone's idea of the tuple state.  Also, since we assume the tuple is
 * not HEAPTUPLE_DEAD, the fact that an XID is not still running allows us
 * to assume that it is either committed good or aborted, as appropriate;
 * so we need no external state checks to decide what to do.  (This is good
 * because this function is applied during WAL recovery, when we don't have
 * access to any such state, and can't depend on the hint bits to be set.)
 *
 * If the tuple is in a shared buffer, caller must hold an exclusive lock on
 * that buffer.
 *
 * Note: it might seem we could make the changes without exclusive lock, since
 * TransactionId read/write is assumed atomic anyway.  However there is a race
 * condition: someone who just fetched an old XID that we overwrite here could
 * conceivably not finish checking the XID against pg_clog before we finish
 * the VACUUM and perhaps truncate off the part of pg_clog he needs.  Getting
 * exclusive lock ensures no other backend is in process of checking the
 * tuple status.  Also, getting exclusive lock makes it safe to adjust the
 * infomask bits.
 */
bool
heap_freeze_tuple(HeapTupleHeader tuple, TransactionId cutoff_xid)
{
	return false;
}

/*
 * heap_tuple_needs_freeze
 *
 * Check to see whether any of the XID fields of a tuple (xmin, xmax, xvac)
 * are older than the specified cutoff XID.  If so, return TRUE.
 *
 * It doesn't matter whether the tuple is alive or dead, we are checking
 * to see if a tuple needs to be removed or frozen to avoid wraparound.
 */
bool
heap_tuple_needs_freeze(HeapTupleHeader tuple, TransactionId cutoff_xid,
						Buffer buf)
{
	return false;
}

/* ----------------
 *		heap_markpos	- mark scan position
 * ----------------
 */
void
heap_markpos(HeapScanDesc scan)
{
	/* Note: no locking manipulations needed */

	if (scan->rs_ctup.t_data != NULL)
	{
		scan->rs_mctid = scan->rs_ctup.t_self;
		if (scan->rs_pageatatime)
			scan->rs_mindex = scan->rs_cindex;
	}
	else
		ItemPointerSetInvalid(&scan->rs_mctid);
}

/* ----------------
 *		heap_restrpos	- restore position to marked location
 * ----------------
 */
void
heap_restrpos(HeapScanDesc scan)
{
	/* XXX no amrestrpos checking that ammarkpos called */

	if (!ItemPointerIsValid(&scan->rs_mctid))
	{
		scan->rs_ctup.t_data = NULL;

		/*
		 * unpin scan buffers
		 */
		if (BufferIsValid(scan->rs_cbuf))
			ReleaseBuffer(scan->rs_cbuf);
		scan->rs_cbuf = InvalidBuffer;
		scan->rs_cblock = InvalidBlockNumber;
		scan->rs_inited = false;
	}
	else
	{
		/*
		 * If we reached end of scan, rs_inited will now be false.	We must
		 * reset it to true to keep heapgettup from doing the wrong thing.
		 */
		scan->rs_inited = true;
		scan->rs_ctup.t_self = scan->rs_mctid;
		if (scan->rs_pageatatime)
		{
			scan->rs_cindex = scan->rs_mindex;
			heapgettup_pagemode(scan,
								NoMovementScanDirection,
								0,		/* needn't recheck scan keys */
								NULL);
		}
		else
			heapgettup(scan,
					   NoMovementScanDirection,
					   0,		/* needn't recheck scan keys */
					   NULL);
	}
}

/*
 * If 'tuple' contains any visible XID greater than latestRemovedXid,
 * ratchet forwards latestRemovedXid to the greatest one found.
 * This is used as the basis for generating Hot Standby conflicts, so
 * if a tuple was never visible then removing it should not conflict
 * with queries.
 */
void
HeapTupleHeaderAdvanceLatestRemovedXid(HeapTupleHeader tuple,
									   TransactionId *latestRemovedXid)
{
}

/*
 * Perform XLogInsert to register a heap cleanup info message. These
 * messages are sent once per VACUUM and are required because
 * of the phasing of removal operations during a lazy VACUUM.
 * see comments for vacuum_log_cleanup_info().
 */
XLogRecPtr
log_heap_cleanup_info(RelFileNode rnode, TransactionId latestRemovedXid)
{
	xl_heap_cleanup_info xlrec;
	XLogRecPtr	recptr;
	XLogRecData rdata;

	xlrec.node = rnode;
	xlrec.latestRemovedXid = latestRemovedXid;

	rdata.data = (char *) &xlrec;
	rdata.len = SizeOfHeapCleanupInfo;
	rdata.buffer = InvalidBuffer;
	rdata.next = NULL;

	recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_CLEANUP_INFO, &rdata);

	return recptr;
}

/*
 * Perform XLogInsert for a heap-clean operation.  Caller must already
 * have modified the buffer and marked it dirty.
 *
 * Note: prior to Postgres 8.3, the entries in the nowunused[] array were
 * zero-based tuple indexes.  Now they are one-based like other uses
 * of OffsetNumber.
 *
 * We also include latestRemovedXid, which is the greatest XID present in
 * the removed tuples. That allows recovery processing to cancel or wait
 * for long standby queries that can still see these tuples.
 */
XLogRecPtr
log_heap_clean(Relation reln, Buffer buffer,
			   OffsetNumber *redirected, int nredirected,
			   OffsetNumber *nowdead, int ndead,
			   OffsetNumber *nowunused, int nunused,
			   TransactionId latestRemovedXid)
{
	xl_heap_clean xlrec;
	uint8		info;
	XLogRecPtr	recptr;
	XLogRecData rdata[4];

	/* Caller should not call me on a non-WAL-logged relation */
	Assert(RelationNeedsWAL(reln));

	xlrec.node = reln->rd_node;
	xlrec.block = BufferGetBlockNumber(buffer);
	xlrec.latestRemovedXid = latestRemovedXid;
	xlrec.nredirected = nredirected;
	xlrec.ndead = ndead;

	rdata[0].data = (char *) &xlrec;
	rdata[0].len = SizeOfHeapClean;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = &(rdata[1]);

	/*
	 * The OffsetNumber arrays are not actually in the buffer, but we pretend
	 * that they are.  When XLogInsert stores the whole buffer, the offset
	 * arrays need not be stored too.  Note that even if all three arrays are
	 * empty, we want to expose the buffer as a candidate for whole-page
	 * storage, since this record type implies a defragmentation operation
	 * even if no item pointers changed state.
	 */
	if (nredirected > 0)
	{
		rdata[1].data = (char *) redirected;
		rdata[1].len = nredirected * sizeof(OffsetNumber) * 2;
	}
	else
	{
		rdata[1].data = NULL;
		rdata[1].len = 0;
	}
	rdata[1].buffer = buffer;
	rdata[1].buffer_std = true;
	rdata[1].next = &(rdata[2]);

	if (ndead > 0)
	{
		rdata[2].data = (char *) nowdead;
		rdata[2].len = ndead * sizeof(OffsetNumber);
	}
	else
	{
		rdata[2].data = NULL;
		rdata[2].len = 0;
	}
	rdata[2].buffer = buffer;
	rdata[2].buffer_std = true;
	rdata[2].next = &(rdata[3]);

	if (nunused > 0)
	{
		rdata[3].data = (char *) nowunused;
		rdata[3].len = nunused * sizeof(OffsetNumber);
	}
	else
	{
		rdata[3].data = NULL;
		rdata[3].len = 0;
	}
	rdata[3].buffer = buffer;
	rdata[3].buffer_std = true;
	rdata[3].next = NULL;

	info = XLOG_HEAP2_CLEAN;
	recptr = XLogInsert(RM_HEAP2_ID, info, rdata);

	return recptr;
}

/*
 * Perform XLogInsert for a heap-freeze operation.	Caller must already
 * have modified the buffer and marked it dirty.
 */
XLogRecPtr
log_heap_freeze(Relation reln, Buffer buffer,
				TransactionId cutoff_xid,
				OffsetNumber *offsets, int offcnt)
{
	xl_heap_freeze xlrec;
	XLogRecPtr	recptr;
	XLogRecData rdata[2];

	/* Caller should not call me on a non-WAL-logged relation */
	Assert(RelationNeedsWAL(reln));
	/* nor when there are no tuples to freeze */
	Assert(offcnt > 0);

	xlrec.node = reln->rd_node;
	xlrec.block = BufferGetBlockNumber(buffer);
	xlrec.cutoff_xid = cutoff_xid;

	rdata[0].data = (char *) &xlrec;
	rdata[0].len = SizeOfHeapFreeze;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = &(rdata[1]);

	/*
	 * The tuple-offsets array is not actually in the buffer, but pretend that
	 * it is.  When XLogInsert stores the whole buffer, the offsets array need
	 * not be stored too.
	 */
	rdata[1].data = (char *) offsets;
	rdata[1].len = offcnt * sizeof(OffsetNumber);
	rdata[1].buffer = buffer;
	rdata[1].buffer_std = true;
	rdata[1].next = NULL;

	recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_FREEZE, rdata);

	return recptr;
}

/*
 * Perform XLogInsert for a heap-visible operation.  'block' is the block
 * being marked all-visible, and vm_buffer is the buffer containing the
 * corresponding visibility map block.	Both should have already been modified
 * and dirtied.
 */
XLogRecPtr
log_heap_visible(RelFileNode rnode, BlockNumber block, Buffer vm_buffer,
				 TransactionId cutoff_xid)
{
	xl_heap_visible xlrec;
	XLogRecPtr	recptr;
	XLogRecData rdata[2];

	xlrec.node = rnode;
	xlrec.block = block;
	xlrec.cutoff_xid = cutoff_xid;

	rdata[0].data = (char *) &xlrec;
	rdata[0].len = SizeOfHeapVisible;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = &(rdata[1]);

	rdata[1].data = NULL;
	rdata[1].len = 0;
	rdata[1].buffer = vm_buffer;
	rdata[1].buffer_std = false;
	rdata[1].next = NULL;

	recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_VISIBLE, rdata);

	return recptr;
}

/*
 * Perform XLogInsert for a heap-update operation.	Caller must already
 * have modified the buffer(s) and marked them dirty.
 */
static XLogRecPtr
log_heap_update(Relation reln, Buffer oldbuf, ItemPointerData from,
				Buffer newbuf, HeapTuple newtup,
				bool all_visible_cleared, bool new_all_visible_cleared)
{
	XLogRecPtr	RecPtr;
	RecPtr.xlogid = 0;
	RecPtr.xrecoff = 0;
	return RecPtr;
}

/*
 * Perform XLogInsert of a HEAP_NEWPAGE record to WAL. Caller is responsible
 * for writing the page to disk after calling this routine.
 *
 * Note: all current callers build pages in private memory and write them
 * directly to smgr, rather than using bufmgr.	Therefore there is no need
 * to pass a buffer ID to XLogInsert, nor to perform MarkBufferDirty within
 * the critical section.
 *
 * Note: the NEWPAGE log record is used for both heaps and indexes, so do
 * not do anything that assumes we are touching a heap.
 */
XLogRecPtr
log_newpage(RelFileNode *rnode, ForkNumber forkNum, BlockNumber blkno,
			Page page)
{
	xl_heap_newpage xlrec;
	XLogRecPtr	recptr;
	XLogRecData rdata[2];

	/* NO ELOG(ERROR) from here till newpage op is logged */
	START_CRIT_SECTION();

	xlrec.node = *rnode;
	xlrec.forknum = forkNum;
	xlrec.blkno = blkno;

	rdata[0].data = (char *) &xlrec;
	rdata[0].len = SizeOfHeapNewpage;
	rdata[0].buffer = InvalidBuffer;
	rdata[0].next = &(rdata[1]);

	rdata[1].data = (char *) page;
	rdata[1].len = BLCKSZ;
	rdata[1].buffer = InvalidBuffer;
	rdata[1].next = NULL;

	recptr = XLogInsert(RM_HEAP_ID, XLOG_HEAP_NEWPAGE, rdata);

	/*
	 * The page may be uninitialized. If so, we can't set the LSN and TLI
	 * because that would corrupt the page.
	 */
	if (!PageIsNew(page))
	{
		PageSetLSN(page, recptr);
		PageSetTLI(page, ThisTimeLineID);
	}

	END_CRIT_SECTION();

	return recptr;
}

/*
 * Handles CLEANUP_INFO
 */
static void
heap_xlog_cleanup_info(XLogRecPtr lsn, XLogRecord *record)
{
	xl_heap_cleanup_info *xlrec = (xl_heap_cleanup_info *) XLogRecGetData(record);

	if (InHotStandby)
		ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid, xlrec->node);

	/*
	 * Actual operation is a no-op. Record type exists to provide a means for
	 * conflict processing to occur before we begin index vacuum actions. see
	 * vacuumlazy.c and also comments in btvacuumpage()
	 */

	/* Backup blocks are not used in cleanup_info records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));
}

/*
 * Handles HEAP2_CLEAN record type
 */
static void
heap_xlog_clean(XLogRecPtr lsn, XLogRecord *record)
{
	xl_heap_clean *xlrec = (xl_heap_clean *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	OffsetNumber *end;
	OffsetNumber *redirected;
	OffsetNumber *nowdead;
	OffsetNumber *nowunused;
	int			nredirected;
	int			ndead;
	int			nunused;
	Size		freespace;

	/*
	 * We're about to remove tuples. In Hot Standby mode, ensure that there's
	 * no queries running for which the removed tuples are still visible.
	 *
	 * Not all HEAP2_CLEAN records remove tuples with xids, so we only want to
	 * conflict on the records that cause MVCC failures for user queries. If
	 * latestRemovedXid is invalid, skip conflict processing.
	 */
	if (InHotStandby && TransactionIdIsValid(xlrec->latestRemovedXid))
		ResolveRecoveryConflictWithSnapshot(xlrec->latestRemovedXid,
											xlrec->node);

	/*
	 * If we have a full-page image, restore it (using a cleanup lock) and
	 * we're done.
	 */
	if (record->xl_info & XLR_BKP_BLOCK(0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, true, false);
		return;
	}

	buffer = XLogReadBufferExtended(xlrec->node, MAIN_FORKNUM, xlrec->block, RBM_NORMAL);
	if (!BufferIsValid(buffer))
		return;
	LockBufferForCleanup(buffer);
	page = (Page) BufferGetPage(buffer);

	if (XLByteLE(lsn, PageGetLSN(page)))
	{
		UnlockReleaseBuffer(buffer);
		return;
	}

	nredirected = xlrec->nredirected;
	ndead = xlrec->ndead;
	end = (OffsetNumber *) ((char *) xlrec + record->xl_len);
	redirected = (OffsetNumber *) ((char *) xlrec + SizeOfHeapClean);
	nowdead = redirected + (nredirected * 2);
	nowunused = nowdead + ndead;
	nunused = (end - nowunused);
	Assert(nunused >= 0);

	/* Update all item pointers per the record, and repair fragmentation */
	heap_page_prune_execute(buffer,
							redirected, nredirected,
							nowdead, ndead,
							nowunused, nunused);

	freespace = PageGetHeapFreeSpace(page);		/* needed to update FSM below */

	/*
	 * Note: we don't worry about updating the page's prunability hints. At
	 * worst this will cause an extra prune cycle to occur soon.
	 */

	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	/*
	 * Update the FSM as well.
	 *
	 * XXX: We don't get here if the page was restored from full page image.
	 * We don't bother to update the FSM in that case, it doesn't need to be
	 * totally accurate anyway.
	 */
	XLogRecordPageWithFreeSpace(xlrec->node, xlrec->block, freespace);
}

static void
heap_xlog_freeze(XLogRecPtr lsn, XLogRecord *record)
{
	xl_heap_freeze *xlrec = (xl_heap_freeze *) XLogRecGetData(record);
	TransactionId cutoff_xid = xlrec->cutoff_xid;
	Buffer		buffer;
	Page		page;

	/*
	 * In Hot Standby mode, ensure that there's no queries running which still
	 * consider the frozen xids as running.
	 */
	if (InHotStandby)
		ResolveRecoveryConflictWithSnapshot(cutoff_xid, xlrec->node);

	/* If we have a full-page image, restore it and we're done */
	if (record->xl_info & XLR_BKP_BLOCK(0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
		return;
	}

	buffer = XLogReadBuffer(xlrec->node, xlrec->block, false);
	if (!BufferIsValid(buffer))
		return;
	page = (Page) BufferGetPage(buffer);

	if (XLByteLE(lsn, PageGetLSN(page)))
	{
		UnlockReleaseBuffer(buffer);
		return;
	}

	if (record->xl_len > SizeOfHeapFreeze)
	{
		OffsetNumber *offsets;
		OffsetNumber *offsets_end;

		offsets = (OffsetNumber *) ((char *) xlrec + SizeOfHeapFreeze);
		offsets_end = (OffsetNumber *) ((char *) xlrec + record->xl_len);

		while (offsets < offsets_end)
		{
			/* offsets[] entries are one-based */
			ItemId		lp = PageGetItemId(page, *offsets);
			HeapTupleHeader tuple = (HeapTupleHeader) PageGetItem(page, lp);

			(void) heap_freeze_tuple(tuple, cutoff_xid);
			offsets++;
		}
	}

	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

/*
 * Replay XLOG_HEAP2_VISIBLE record.
 *
 * The critical integrity requirement here is that we must never end up with
 * a situation where the visibility map bit is set, and the page-level
 * PD_ALL_VISIBLE bit is clear.  If that were to occur, then a subsequent
 * page modification would fail to clear the visibility map bit.
 */
static void
heap_xlog_visible(XLogRecPtr lsn, XLogRecord *record)
{
	xl_heap_visible *xlrec = (xl_heap_visible *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;

	/*
	 * If there are any Hot Standby transactions running that have an xmin
	 * horizon old enough that this page isn't all-visible for them, they
	 * might incorrectly decide that an index-only scan can skip a heap fetch.
	 *
	 * NB: It might be better to throw some kind of "soft" conflict here that
	 * forces any index-only scan that is in flight to perform heap fetches,
	 * rather than killing the transaction outright.
	 */
	if (InHotStandby)
		ResolveRecoveryConflictWithSnapshot(xlrec->cutoff_xid, xlrec->node);

	/*
	 * Read the heap page, if it still exists.	If the heap file has been
	 * dropped or truncated later in recovery, we don't need to update the
	 * page, but we'd better still update the visibility map.
	 */
	buffer = XLogReadBufferExtended(xlrec->node, MAIN_FORKNUM, xlrec->block,
									RBM_NORMAL);
	if (BufferIsValid(buffer))
	{
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

		page = (Page) BufferGetPage(buffer);

		/*
		 * We don't bump the LSN of the heap page when setting the visibility
		 * map bit, because that would generate an unworkable volume of
		 * full-page writes.  This exposes us to torn page hazards, but since
		 * we're not inspecting the existing page contents in any way, we
		 * don't care.
		 *
		 * However, all operations that clear the visibility map bit *do* bump
		 * the LSN, and those operations will only be replayed if the XLOG LSN
		 * follows the page LSN.  Thus, if the page LSN has advanced past our
		 * XLOG record's LSN, we mustn't mark the page all-visible, because
		 * the subsequent update won't be replayed to clear the flag.
		 */
		if (!XLByteLE(lsn, PageGetLSN(page)))
		{
			PageSetAllVisible(page);
			MarkBufferDirty(buffer);
		}

		/* Done with heap page. */
		UnlockReleaseBuffer(buffer);
	}

	/*
	 * Even if we skipped the heap page update due to the LSN interlock, it's
	 * still safe to update the visibility map.  Any WAL record that clears
	 * the visibility map bit does so before checking the page LSN, so any
	 * bits that need to be cleared will still be cleared.
	 */
	if (record->xl_info & XLR_BKP_BLOCK(0))
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
	else
	{
		Relation	reln;
		Buffer		vmbuffer = InvalidBuffer;

		reln = CreateFakeRelcacheEntry(xlrec->node);
		visibilitymap_pin(reln, xlrec->block, &vmbuffer);

		/*
		 * Don't set the bit if replay has already passed this point.
		 *
		 * It might be safe to do this unconditionally; if replay has passed
		 * this point, we'll replay at least as far this time as we did
		 * before, and if this bit needs to be cleared, the record responsible
		 * for doing so should be again replayed, and clear it.  For right
		 * now, out of an abundance of conservatism, we use the same test here
		 * we did for the heap page.  If this results in a dropped bit, no
		 * real harm is done; and the next VACUUM will fix it.
		 */
		if (!XLByteLE(lsn, PageGetLSN(BufferGetPage(vmbuffer))))
			visibilitymap_set(reln, xlrec->block, lsn, vmbuffer,
							  xlrec->cutoff_xid);

		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}
}

static void
heap_xlog_newpage(XLogRecPtr lsn, XLogRecord *record)
{
	xl_heap_newpage *xlrec = (xl_heap_newpage *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;

	/* Backup blocks are not used in newpage records */
	Assert(!(record->xl_info & XLR_BKP_BLOCK_MASK));

	/*
	 * Note: the NEWPAGE log record is used for both heaps and indexes, so do
	 * not do anything that assumes we are touching a heap.
	 */
	buffer = XLogReadBufferExtended(xlrec->node, xlrec->forknum, xlrec->blkno,
									RBM_ZERO);
	Assert(BufferIsValid(buffer));
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = (Page) BufferGetPage(buffer);

	Assert(record->xl_len == SizeOfHeapNewpage + BLCKSZ);
	memcpy(page, (char *) xlrec + SizeOfHeapNewpage, BLCKSZ);

	/*
	 * The page may be uninitialized. If so, we can't set the LSN and TLI
	 * because that would corrupt the page.
	 */
	if (!PageIsNew(page))
	{
		PageSetLSN(page, lsn);
		PageSetTLI(page, ThisTimeLineID);
	}

	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

static void
heap_xlog_delete(XLogRecPtr lsn, XLogRecord *record)
{
	xl_heap_delete *xlrec = (xl_heap_delete *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	BlockNumber blkno;

	blkno = ItemPointerGetBlockNumber(&(xlrec->target.tid));

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->all_visible_cleared)
	{
		Relation	reln = CreateFakeRelcacheEntry(xlrec->target.node);
		Buffer		vmbuffer = InvalidBuffer;

		visibilitymap_pin(reln, blkno, &vmbuffer);
		visibilitymap_clear(reln, blkno, vmbuffer);
		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}

	/* If we have a full-page image, restore it and we're done */
	if (record->xl_info & XLR_BKP_BLOCK(0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
		return;
	}

	buffer = XLogReadBuffer(xlrec->target.node, blkno, false);
	if (!BufferIsValid(buffer))
		return;
	page = (Page) BufferGetPage(buffer);

	if (XLByteLE(lsn, PageGetLSN(page)))		/* changes are applied */
	{
		UnlockReleaseBuffer(buffer);
		return;
	}

	offnum = ItemPointerGetOffsetNumber(&(xlrec->target.tid));
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
		elog(PANIC, "heap_delete_redo: invalid lp");

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	htup->t_infomask &= ~(HEAP_XMAX_COMMITTED |
						  HEAP_XMAX_INVALID |
						  HEAP_XMAX_IS_MULTI |
						  HEAP_IS_LOCKED |
						  HEAP_MOVED);
	HeapTupleHeaderClearHotUpdated(htup);
	HeapTupleHeaderSetXmax(htup, record->xl_xid);
	HeapTupleHeaderSetCmax(htup, FirstCommandId, false);

	/* Mark the page as a candidate for pruning */
	PageSetPrunable(page, record->xl_xid);

	if (xlrec->all_visible_cleared)
		PageClearAllVisible(page);

	/* Make sure there is no forward chain link in t_ctid */
	htup->t_ctid = xlrec->target.tid;
	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

static void
heap_xlog_insert(XLogRecPtr lsn, XLogRecord *record)
{
	xl_heap_insert *xlrec = (xl_heap_insert *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	struct
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	HeapTupleHeader htup;
	xl_heap_header xlhdr;
	uint32		newlen;
	Size		freespace;
	BlockNumber blkno;

	elog(DEBUG4, "Gone through xlog");

	blkno = ItemPointerGetBlockNumber(&(xlrec->target.tid));

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->all_visible_cleared)
	{
		Relation	reln = CreateFakeRelcacheEntry(xlrec->target.node);
		Buffer		vmbuffer = InvalidBuffer;

		visibilitymap_pin(reln, blkno, &vmbuffer);
		visibilitymap_clear(reln, blkno, vmbuffer);
		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}

	/* If we have a full-page image, restore it and we're done */
	if (record->xl_info & XLR_BKP_BLOCK(0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
		return;
	}

	if (record->xl_info & XLOG_HEAP_INIT_PAGE)
	{
		buffer = XLogReadBuffer(xlrec->target.node, blkno, true);
		Assert(BufferIsValid(buffer));
		page = (Page) BufferGetPage(buffer);

		PageInit(page, BufferGetPageSize(buffer), 0);
	}
	else
	{
		buffer = XLogReadBuffer(xlrec->target.node, blkno, false);
		if (!BufferIsValid(buffer))
			return;
		page = (Page) BufferGetPage(buffer);

		if (XLByteLE(lsn, PageGetLSN(page)))	/* changes are applied */
		{
			UnlockReleaseBuffer(buffer);
			return;
		}
	}

	offnum = ItemPointerGetOffsetNumber(&(xlrec->target.tid));
	if (PageGetMaxOffsetNumber(page) + 1 < offnum)
		elog(PANIC, "heap_insert_redo: invalid max offset number");

	newlen = record->xl_len - SizeOfHeapInsert - SizeOfHeapHeader;
	Assert(newlen <= MaxHeapTupleSize);
	memcpy((char *) &xlhdr,
		   (char *) xlrec + SizeOfHeapInsert,
		   SizeOfHeapHeader);
	htup = &tbuf.hdr;
	MemSet((char *) htup, 0, sizeof(HeapTupleHeaderData));
	/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
	memcpy((char *) htup + offsetof(HeapTupleHeaderData, t_bits),
		   (char *) xlrec + SizeOfHeapInsert + SizeOfHeapHeader,
		   newlen);
	newlen += offsetof(HeapTupleHeaderData, t_bits);
	htup->t_infomask2 = xlhdr.t_infomask2;
	htup->t_infomask = xlhdr.t_infomask;
	htup->t_hoff = xlhdr.t_hoff;
	HeapTupleHeaderSetXmin(htup, record->xl_xid);
	HeapTupleHeaderSetCmin(htup, FirstCommandId);
	htup->t_ctid = xlrec->target.tid;

	offnum = PageAddItem(page, (Item) htup, newlen, offnum, true, true);
	if (offnum == InvalidOffsetNumber)
		elog(PANIC, "heap_insert_redo: failed to add tuple");

	freespace = PageGetHeapFreeSpace(page);		/* needed to update FSM below */

	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);

	if (xlrec->all_visible_cleared)
		PageClearAllVisible(page);

	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	/*
	 * If the page is running low on free space, update the FSM as well.
	 * Arbitrarily, our definition of "low" is less than 20%. We can't do much
	 * better than that without knowing the fill-factor for the table.
	 *
	 * XXX: We don't get here if the page was restored from full page image.
	 * We don't bother to update the FSM in that case, it doesn't need to be
	 * totally accurate anyway.
	 */
	if (freespace < BLCKSZ / 5)
		XLogRecordPageWithFreeSpace(xlrec->target.node, blkno, freespace);
}

/*
 * Handles MULTI_INSERT record type.
 */
static void
heap_xlog_multi_insert(XLogRecPtr lsn, XLogRecord *record)
{
	char	   *recdata = XLogRecGetData(record);
	xl_heap_multi_insert *xlrec;
	Buffer		buffer;
	Page		page;
	struct
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	HeapTupleHeader htup;
	uint32		newlen;
	Size		freespace;
	BlockNumber blkno;
	int			i;
	bool		isinit = (record->xl_info & XLOG_HEAP_INIT_PAGE) != 0;

	elog(DEBUG4, "multi insert");
	/*
	 * Insertion doesn't overwrite MVCC data, so no conflict processing is
	 * required.
	 */

	xlrec = (xl_heap_multi_insert *) recdata;
	recdata += SizeOfHeapMultiInsert;

	/*
	 * If we're reinitializing the page, the tuples are stored in order from
	 * FirstOffsetNumber. Otherwise there's an array of offsets in the WAL
	 * record.
	 */
	if (!isinit)
		recdata += sizeof(OffsetNumber) * xlrec->ntuples;

	blkno = xlrec->blkno;

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->all_visible_cleared)
	{
		Relation	reln = CreateFakeRelcacheEntry(xlrec->node);
		Buffer		vmbuffer = InvalidBuffer;

		visibilitymap_pin(reln, blkno, &vmbuffer);
		visibilitymap_clear(reln, blkno, vmbuffer);
		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}

	/* If we have a full-page image, restore it and we're done */
	if (record->xl_info & XLR_BKP_BLOCK(0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
		return;
	}

	if (isinit)
	{
		buffer = XLogReadBuffer(xlrec->node, blkno, true);
		Assert(BufferIsValid(buffer));
		page = (Page) BufferGetPage(buffer);

		PageInit(page, BufferGetPageSize(buffer), 0);
	}
	else
	{
		buffer = XLogReadBuffer(xlrec->node, blkno, false);
		if (!BufferIsValid(buffer))
			return;
		page = (Page) BufferGetPage(buffer);

		if (XLByteLE(lsn, PageGetLSN(page)))	/* changes are applied */
		{
			UnlockReleaseBuffer(buffer);
			return;
		}
	}

	for (i = 0; i < xlrec->ntuples; i++)
	{
		OffsetNumber offnum;
		xl_multi_insert_tuple *xlhdr;

		if (isinit)
			offnum = FirstOffsetNumber + i;
		else
			offnum = xlrec->offsets[i];
		if (PageGetMaxOffsetNumber(page) + 1 < offnum)
			elog(PANIC, "heap_multi_insert_redo: invalid max offset number");

		xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(recdata);
		recdata = ((char *) xlhdr) + SizeOfMultiInsertTuple;

		newlen = xlhdr->datalen;
		Assert(newlen <= MaxHeapTupleSize);
		htup = &tbuf.hdr;
		MemSet((char *) htup, 0, sizeof(HeapTupleHeaderData));
		/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
		memcpy((char *) htup + offsetof(HeapTupleHeaderData, t_bits),
			   (char *) recdata,
			   newlen);
		recdata += newlen;

		newlen += offsetof(HeapTupleHeaderData, t_bits);
		htup->t_infomask2 = xlhdr->t_infomask2;
		htup->t_infomask = xlhdr->t_infomask;
		htup->t_hoff = xlhdr->t_hoff;
		HeapTupleHeaderSetXmin(htup, record->xl_xid);
		HeapTupleHeaderSetCmin(htup, FirstCommandId);
		ItemPointerSetBlockNumber(&htup->t_ctid, blkno);
		ItemPointerSetOffsetNumber(&htup->t_ctid, offnum);

		offnum = PageAddItem(page, (Item) htup, newlen, offnum, true, true);
		if (offnum == InvalidOffsetNumber)
			elog(PANIC, "heap_multi_insert_redo: failed to add tuple");
	}

	freespace = PageGetHeapFreeSpace(page);		/* needed to update FSM below */

	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);

	if (xlrec->all_visible_cleared)
		PageClearAllVisible(page);

	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	/*
	 * If the page is running low on free space, update the FSM as well.
	 * Arbitrarily, our definition of "low" is less than 20%. We can't do much
	 * better than that without knowing the fill-factor for the table.
	 *
	 * XXX: We don't get here if the page was restored from full page image.
	 * We don't bother to update the FSM in that case, it doesn't need to be
	 * totally accurate anyway.
	 */
	if (freespace < BLCKSZ / 5)
		XLogRecordPageWithFreeSpace(xlrec->node, blkno, freespace);
}

/*
 * Handles UPDATE and HOT_UPDATE
 */
static void
heap_xlog_update(XLogRecPtr lsn, XLogRecord *record, bool hot_update)
{
	xl_heap_update *xlrec = (xl_heap_update *) XLogRecGetData(record);
	bool		samepage = (ItemPointerGetBlockNumber(&(xlrec->newtid)) ==
							ItemPointerGetBlockNumber(&(xlrec->target.tid)));
	Buffer		obuffer,
				nbuffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	struct
	{
		HeapTupleHeaderData hdr;
		char		data[MaxHeapTupleSize];
	}			tbuf;
	xl_heap_header xlhdr;
	int			hsize;
	uint32		newlen;
	Size		freespace;

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->all_visible_cleared)
	{
		Relation	reln = CreateFakeRelcacheEntry(xlrec->target.node);
		BlockNumber block = ItemPointerGetBlockNumber(&xlrec->target.tid);
		Buffer		vmbuffer = InvalidBuffer;

		visibilitymap_pin(reln, block, &vmbuffer);
		visibilitymap_clear(reln, block, vmbuffer);
		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}

	/*
	 * In normal operation, it is important to lock the two pages in
	 * page-number order, to avoid possible deadlocks against other update
	 * operations going the other way.	However, during WAL replay there can
	 * be no other update happening, so we don't need to worry about that. But
	 * we *do* need to worry that we don't expose an inconsistent state to Hot
	 * Standby queries --- so the original page can't be unlocked before we've
	 * added the new tuple to the new page.
	 */

	if (record->xl_info & XLR_BKP_BLOCK(0))
	{
		obuffer = RestoreBackupBlock(lsn, record, 0, false, true);
		if (samepage)
		{
			/* backup block covered both changes, so we're done */
			UnlockReleaseBuffer(obuffer);
			return;
		}
		goto newt;
	}

	/* Deal with old tuple version */

	obuffer = XLogReadBuffer(xlrec->target.node,
							 ItemPointerGetBlockNumber(&(xlrec->target.tid)),
							 false);
	if (!BufferIsValid(obuffer))
		goto newt;
	page = (Page) BufferGetPage(obuffer);

	if (XLByteLE(lsn, PageGetLSN(page)))		/* changes are applied */
	{
		if (samepage)
		{
			UnlockReleaseBuffer(obuffer);
			return;
		}
		goto newt;
	}

	offnum = ItemPointerGetOffsetNumber(&(xlrec->target.tid));
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
		elog(PANIC, "heap_update_redo: invalid lp");

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	htup->t_infomask &= ~(HEAP_XMAX_COMMITTED |
						  HEAP_XMAX_INVALID |
						  HEAP_XMAX_IS_MULTI |
						  HEAP_IS_LOCKED |
						  HEAP_MOVED);
	if (hot_update)
		HeapTupleHeaderSetHotUpdated(htup);
	else
		HeapTupleHeaderClearHotUpdated(htup);
	HeapTupleHeaderSetXmax(htup, record->xl_xid);
	HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
	/* Set forward chain link in t_ctid */
	htup->t_ctid = xlrec->newtid;

	/* Mark the page as a candidate for pruning */
	PageSetPrunable(page, record->xl_xid);

	if (xlrec->all_visible_cleared)
		PageClearAllVisible(page);

	/*
	 * this test is ugly, but necessary to avoid thinking that insert change
	 * is already applied
	 */
	if (samepage)
	{
		nbuffer = obuffer;
		goto newsame;
	}

	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(obuffer);

	/* Deal with new tuple */

newt:;

	/*
	 * The visibility map may need to be fixed even if the heap page is
	 * already up-to-date.
	 */
	if (xlrec->new_all_visible_cleared)
	{
		Relation	reln = CreateFakeRelcacheEntry(xlrec->target.node);
		BlockNumber block = ItemPointerGetBlockNumber(&xlrec->newtid);
		Buffer		vmbuffer = InvalidBuffer;

		visibilitymap_pin(reln, block, &vmbuffer);
		visibilitymap_clear(reln, block, vmbuffer);
		ReleaseBuffer(vmbuffer);
		FreeFakeRelcacheEntry(reln);
	}

	if (record->xl_info & XLR_BKP_BLOCK(1))
	{
		(void) RestoreBackupBlock(lsn, record, 1, false, false);
		if (BufferIsValid(obuffer))
			UnlockReleaseBuffer(obuffer);
		return;
	}

	if (record->xl_info & XLOG_HEAP_INIT_PAGE)
	{
		nbuffer = XLogReadBuffer(xlrec->target.node,
								 ItemPointerGetBlockNumber(&(xlrec->newtid)),
								 true);
		Assert(BufferIsValid(nbuffer));
		page = (Page) BufferGetPage(nbuffer);

		PageInit(page, BufferGetPageSize(nbuffer), 0);
	}
	else
	{
		nbuffer = XLogReadBuffer(xlrec->target.node,
								 ItemPointerGetBlockNumber(&(xlrec->newtid)),
								 false);
		if (!BufferIsValid(nbuffer))
			return;
		page = (Page) BufferGetPage(nbuffer);

		if (XLByteLE(lsn, PageGetLSN(page)))	/* changes are applied */
		{
			UnlockReleaseBuffer(nbuffer);
			if (BufferIsValid(obuffer))
				UnlockReleaseBuffer(obuffer);
			return;
		}
	}

newsame:;

	offnum = ItemPointerGetOffsetNumber(&(xlrec->newtid));
	if (PageGetMaxOffsetNumber(page) + 1 < offnum)
		elog(PANIC, "heap_update_redo: invalid max offset number");

	hsize = SizeOfHeapUpdate + SizeOfHeapHeader;

	newlen = record->xl_len - hsize;
	Assert(newlen <= MaxHeapTupleSize);
	memcpy((char *) &xlhdr,
		   (char *) xlrec + SizeOfHeapUpdate,
		   SizeOfHeapHeader);
	htup = &tbuf.hdr;
	MemSet((char *) htup, 0, sizeof(HeapTupleHeaderData));
	/* PG73FORMAT: get bitmap [+ padding] [+ oid] + data */
	memcpy((char *) htup + offsetof(HeapTupleHeaderData, t_bits),
		   (char *) xlrec + hsize,
		   newlen);
	newlen += offsetof(HeapTupleHeaderData, t_bits);
	htup->t_infomask2 = xlhdr.t_infomask2;
	htup->t_infomask = xlhdr.t_infomask;
	htup->t_hoff = xlhdr.t_hoff;

	HeapTupleHeaderSetXmin(htup, record->xl_xid);
	HeapTupleHeaderSetCmin(htup, FirstCommandId);
	/* Make sure there is no forward chain link in t_ctid */
	htup->t_ctid = xlrec->newtid;

	offnum = PageAddItem(page, (Item) htup, newlen, offnum, true, true);
	if (offnum == InvalidOffsetNumber)
		elog(PANIC, "heap_update_redo: failed to add tuple");

	if (xlrec->new_all_visible_cleared)
		PageClearAllVisible(page);

	freespace = PageGetHeapFreeSpace(page);		/* needed to update FSM below */

	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(nbuffer);
	UnlockReleaseBuffer(nbuffer);

	if (BufferIsValid(obuffer) && obuffer != nbuffer)
		UnlockReleaseBuffer(obuffer);

	/*
	 * If the new page is running low on free space, update the FSM as well.
	 * Arbitrarily, our definition of "low" is less than 20%. We can't do much
	 * better than that without knowing the fill-factor for the table.
	 *
	 * However, don't update the FSM on HOT updates, because after crash
	 * recovery, either the old or the new tuple will certainly be dead and
	 * prunable. After pruning, the page will have roughly as much free space
	 * as it did before the update, assuming the new tuple is about the same
	 * size as the old one.
	 *
	 * XXX: We don't get here if the page was restored from full page image.
	 * We don't bother to update the FSM in that case, it doesn't need to be
	 * totally accurate anyway.
	 */
	if (!hot_update && freespace < BLCKSZ / 5)
		XLogRecordPageWithFreeSpace(xlrec->target.node,
								 ItemPointerGetBlockNumber(&(xlrec->newtid)),
									freespace);
}

static void
heap_xlog_lock(XLogRecPtr lsn, XLogRecord *record)
{
	xl_heap_lock *xlrec = (xl_heap_lock *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;

	/* If we have a full-page image, restore it and we're done */
	if (record->xl_info & XLR_BKP_BLOCK(0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
		return;
	}

	buffer = XLogReadBuffer(xlrec->target.node,
							ItemPointerGetBlockNumber(&(xlrec->target.tid)),
							false);
	if (!BufferIsValid(buffer))
		return;
	page = (Page) BufferGetPage(buffer);

	if (XLByteLE(lsn, PageGetLSN(page)))		/* changes are applied */
	{
		UnlockReleaseBuffer(buffer);
		return;
	}

	offnum = ItemPointerGetOffsetNumber(&(xlrec->target.tid));
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
		elog(PANIC, "heap_lock_redo: invalid lp");

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	htup->t_infomask &= ~(HEAP_XMAX_COMMITTED |
						  HEAP_XMAX_INVALID |
						  HEAP_XMAX_IS_MULTI |
						  HEAP_IS_LOCKED |
						  HEAP_MOVED);
	if (xlrec->xid_is_mxact)
		htup->t_infomask |= HEAP_XMAX_IS_MULTI;
	if (xlrec->shared_lock)
		htup->t_infomask |= HEAP_XMAX_SHARED_LOCK;
	else
		htup->t_infomask |= HEAP_XMAX_EXCL_LOCK;
	HeapTupleHeaderClearHotUpdated(htup);
	HeapTupleHeaderSetXmax(htup, xlrec->locking_xid);
	HeapTupleHeaderSetCmax(htup, FirstCommandId, false);
	/* Make sure there is no forward chain link in t_ctid */
	htup->t_ctid = xlrec->target.tid;
	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

static void
heap_xlog_inplace(XLogRecPtr lsn, XLogRecord *record)
{
	xl_heap_inplace *xlrec = (xl_heap_inplace *) XLogRecGetData(record);
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	ItemId		lp = NULL;
	HeapTupleHeader htup;
	uint32		oldlen;
	uint32		newlen;

	/* If we have a full-page image, restore it and we're done */
	if (record->xl_info & XLR_BKP_BLOCK(0))
	{
		(void) RestoreBackupBlock(lsn, record, 0, false, false);
		return;
	}

	buffer = XLogReadBuffer(xlrec->target.node,
							ItemPointerGetBlockNumber(&(xlrec->target.tid)),
							false);
	if (!BufferIsValid(buffer))
		return;
	page = (Page) BufferGetPage(buffer);

	if (XLByteLE(lsn, PageGetLSN(page)))		/* changes are applied */
	{
		UnlockReleaseBuffer(buffer);
		return;
	}

	offnum = ItemPointerGetOffsetNumber(&(xlrec->target.tid));
	if (PageGetMaxOffsetNumber(page) >= offnum)
		lp = PageGetItemId(page, offnum);

	if (PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp))
		elog(PANIC, "heap_inplace_redo: invalid lp");

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	oldlen = ItemIdGetLength(lp) - htup->t_hoff;
	newlen = record->xl_len - SizeOfHeapInplace;
	if (oldlen != newlen)
		elog(PANIC, "heap_inplace_redo: wrong tuple length");

	memcpy((char *) htup + htup->t_hoff,
		   (char *) xlrec + SizeOfHeapInplace,
		   newlen);

	PageSetLSN(page, lsn);
	PageSetTLI(page, ThisTimeLineID);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

void
heap_redo(XLogRecPtr lsn, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;

	/*
	 * These operations don't overwrite MVCC data so no conflict processing is
	 * required. The ones in heap2 rmgr do.
	 */

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP_INSERT:
			heap_xlog_insert(lsn, record);
			break;
		case XLOG_HEAP_DELETE:
			heap_xlog_delete(lsn, record);
			break;
		case XLOG_HEAP_UPDATE:
			heap_xlog_update(lsn, record, false);
			break;
		case XLOG_HEAP_HOT_UPDATE:
			heap_xlog_update(lsn, record, true);
			break;
		case XLOG_HEAP_NEWPAGE:
			heap_xlog_newpage(lsn, record);
			break;
		case XLOG_HEAP_LOCK:
			heap_xlog_lock(lsn, record);
			break;
		case XLOG_HEAP_INPLACE:
			heap_xlog_inplace(lsn, record);
			break;
		default:
			elog(PANIC, "heap_redo: unknown op code %u", info);
	}
}

void
heap2_redo(XLogRecPtr lsn, XLogRecord *record)
{
	uint8		info = record->xl_info & ~XLR_INFO_MASK;

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP2_FREEZE:
			heap_xlog_freeze(lsn, record);
			break;
		case XLOG_HEAP2_CLEAN:
			heap_xlog_clean(lsn, record);
			break;
		case XLOG_HEAP2_CLEANUP_INFO:
			heap_xlog_cleanup_info(lsn, record);
			break;
		case XLOG_HEAP2_VISIBLE:
			heap_xlog_visible(lsn, record);
			break;
		case XLOG_HEAP2_MULTI_INSERT:
			heap_xlog_multi_insert(lsn, record);
			break;
		default:
			elog(PANIC, "heap2_redo: unknown op code %u", info);
	}
}

static void
out_target(StringInfo buf, xl_heaptid *target)
{
	appendStringInfo(buf, "rel %u/%u/%u; tid %u/%u",
			 target->node.spcNode, target->node.dbNode, target->node.relNode,
					 ItemPointerGetBlockNumber(&(target->tid)),
					 ItemPointerGetOffsetNumber(&(target->tid)));
}

void
heap_desc(StringInfo buf, uint8 xl_info, char *rec)
{
	uint8		info = xl_info & ~XLR_INFO_MASK;

	info &= XLOG_HEAP_OPMASK;
	if (info == XLOG_HEAP_INSERT)
	{
		xl_heap_insert *xlrec = (xl_heap_insert *) rec;

		if (xl_info & XLOG_HEAP_INIT_PAGE)
			appendStringInfo(buf, "insert(init): ");
		else
			appendStringInfo(buf, "insert: ");
		out_target(buf, &(xlrec->target));
	}
	else if (info == XLOG_HEAP_DELETE)
	{
		xl_heap_delete *xlrec = (xl_heap_delete *) rec;

		appendStringInfo(buf, "delete: ");
		out_target(buf, &(xlrec->target));
	}
	else if (info == XLOG_HEAP_UPDATE)
	{
		xl_heap_update *xlrec = (xl_heap_update *) rec;

		if (xl_info & XLOG_HEAP_INIT_PAGE)
			appendStringInfo(buf, "update(init): ");
		else
			appendStringInfo(buf, "update: ");
		out_target(buf, &(xlrec->target));
		appendStringInfo(buf, "; new %u/%u",
						 ItemPointerGetBlockNumber(&(xlrec->newtid)),
						 ItemPointerGetOffsetNumber(&(xlrec->newtid)));
	}
	else if (info == XLOG_HEAP_HOT_UPDATE)
	{
		xl_heap_update *xlrec = (xl_heap_update *) rec;

		if (xl_info & XLOG_HEAP_INIT_PAGE)		/* can this case happen? */
			appendStringInfo(buf, "hot_update(init): ");
		else
			appendStringInfo(buf, "hot_update: ");
		out_target(buf, &(xlrec->target));
		appendStringInfo(buf, "; new %u/%u",
						 ItemPointerGetBlockNumber(&(xlrec->newtid)),
						 ItemPointerGetOffsetNumber(&(xlrec->newtid)));
	}
	else if (info == XLOG_HEAP_NEWPAGE)
	{
		xl_heap_newpage *xlrec = (xl_heap_newpage *) rec;

		appendStringInfo(buf, "newpage: rel %u/%u/%u; fork %u, blk %u",
						 xlrec->node.spcNode, xlrec->node.dbNode,
						 xlrec->node.relNode, xlrec->forknum,
						 xlrec->blkno);
	}
	else if (info == XLOG_HEAP_LOCK)
	{
		xl_heap_lock *xlrec = (xl_heap_lock *) rec;

		if (xlrec->shared_lock)
			appendStringInfo(buf, "shared_lock: ");
		else
			appendStringInfo(buf, "exclusive_lock: ");
		if (xlrec->xid_is_mxact)
			appendStringInfo(buf, "mxid ");
		else
			appendStringInfo(buf, "xid ");
		appendStringInfo(buf, "%u ", xlrec->locking_xid);
		out_target(buf, &(xlrec->target));
	}
	else if (info == XLOG_HEAP_INPLACE)
	{
		xl_heap_inplace *xlrec = (xl_heap_inplace *) rec;

		appendStringInfo(buf, "inplace: ");
		out_target(buf, &(xlrec->target));
	}
	else
		appendStringInfo(buf, "UNKNOWN");
}

void
heap2_desc(StringInfo buf, uint8 xl_info, char *rec)
{
	uint8		info = xl_info & ~XLR_INFO_MASK;

	info &= XLOG_HEAP_OPMASK;
	if (info == XLOG_HEAP2_FREEZE)
	{
		xl_heap_freeze *xlrec = (xl_heap_freeze *) rec;

		appendStringInfo(buf, "freeze: rel %u/%u/%u; blk %u; cutoff %u",
						 xlrec->node.spcNode, xlrec->node.dbNode,
						 xlrec->node.relNode, xlrec->block,
						 xlrec->cutoff_xid);
	}
	else if (info == XLOG_HEAP2_CLEAN)
	{
		xl_heap_clean *xlrec = (xl_heap_clean *) rec;

		appendStringInfo(buf, "clean: rel %u/%u/%u; blk %u remxid %u",
						 xlrec->node.spcNode, xlrec->node.dbNode,
						 xlrec->node.relNode, xlrec->block,
						 xlrec->latestRemovedXid);
	}
	else if (info == XLOG_HEAP2_CLEANUP_INFO)
	{
		xl_heap_cleanup_info *xlrec = (xl_heap_cleanup_info *) rec;

		appendStringInfo(buf, "cleanup info: remxid %u",
						 xlrec->latestRemovedXid);
	}
	else if (info == XLOG_HEAP2_VISIBLE)
	{
		xl_heap_visible *xlrec = (xl_heap_visible *) rec;

		appendStringInfo(buf, "visible: rel %u/%u/%u; blk %u",
						 xlrec->node.spcNode, xlrec->node.dbNode,
						 xlrec->node.relNode, xlrec->block);
	}
	else if (info == XLOG_HEAP2_MULTI_INSERT)
	{
		xl_heap_multi_insert *xlrec = (xl_heap_multi_insert *) rec;

		if (xl_info & XLOG_HEAP_INIT_PAGE)
			appendStringInfo(buf, "multi-insert (init): ");
		else
			appendStringInfo(buf, "multi-insert: ");
		appendStringInfo(buf, "rel %u/%u/%u; blk %u; %d tuples",
				xlrec->node.spcNode, xlrec->node.dbNode, xlrec->node.relNode,
						 xlrec->blkno, xlrec->ntuples);
	}
	else
		appendStringInfo(buf, "UNKNOWN");
}

/*
 *	heap_sync		- sync a heap, for use when no WAL has been written
 *
 * This forces the heap contents (including TOAST heap if any) down to disk.
 * If we skipped using WAL, and WAL is otherwise needed, we must force the
 * relation down to disk before it's safe to commit the transaction.  This
 * requires writing out any dirty buffers and then doing a forced fsync.
 *
 * Indexes are not touched.  (Currently, index operations associated with
 * the commands that use this are WAL-logged and so do not need fsync.
 * That behavior might change someday, but in any case it's likely that
 * any fsync decisions required would be per-index and hence not appropriate
 * to be done here.)
 */
void
heap_sync(Relation rel)
{
	/* non-WAL-logged tables never need fsync */
	if (!RelationNeedsWAL(rel))
		return;

	/* main heap */
	FlushRelationBuffers(rel);
	/* FlushRelationBuffers will have opened rd_smgr */
	smgrimmedsync(rel->rd_smgr, MAIN_FORKNUM);

	/* FSM is not critical, don't bother syncing it */

	/* toast heap, if any */
	if (OidIsValid(rel->rd_rel->reltoastrelid))
	{
		Relation	toastrel;

		toastrel = heap_open(rel->rd_rel->reltoastrelid, AccessShareLock);
		FlushRelationBuffers(toastrel);
		smgrimmedsync(toastrel->rd_smgr, MAIN_FORKNUM);
		heap_close(toastrel, AccessShareLock);
	}
}
