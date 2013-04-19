/*-------------------------------------------------------------------------
 *
 * bufpage.h
 *	  Standard POSTGRES buffer page definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/bufpage.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUFPAGE_H
#define BUFPAGE_H

#include "access/xlogdefs.h"
#include "storage/item.h"
#include "storage/off.h"

/*
 * A postgres disk page is an abstraction layered on top of a postgres
 * disk block (which is simply a unit of i/o, see block.h).
 *
 * specifically, while a disk block can be unformatted, a postgres
 * disk page is always a slotted page of the form:
 *
 * +----------------+---------------------------------+
 * | PageHeaderData | linp1 linp2 linp3 ...			  |
 * +-----------+----+---------------------------------+
 * | ... linpN |									  |
 * +-----------+--------------------------------------+
 * |		   ^ pd_lower							  |
 * |												  |
 * |			 v pd_upper							  |
 * +-------------+------------------------------------+
 * |			 | tupleN ...						  |
 * +-------------+------------------+-----------------+
 * |	   ... tuple3 tuple2 tuple1 | "special space" |
 * +--------------------------------+-----------------+
 *									^ pd_special
 *
 * a page is full when nothing can be added between pd_lower and
 * pd_upper.
 *
 * all blocks written out by an access method must be disk pages.
 *
 * EXCEPTIONS:
 *
 * obviously, a page is not formatted before it is initialized by
 * a call to PageInit.
 *
 * NOTES:
 *
 * linp1..N form an ItemId array.  ItemPointers point into this array
 * rather than pointing directly to a tuple.  Note that OffsetNumbers
 * conventionally start at 1, not 0.
 *
 * tuple1..N are added "backwards" on the page.  because a tuple's
 * ItemPointer points to its ItemId entry rather than its actual
 * byte-offset position, tuples can be physically shuffled on a page
 * whenever the need arises.
 *
 * AM-generic per-page information is kept in PageHeaderData.
 *
 * AM-specific per-page data (if any) is kept in the area marked "special
 * space"; each AM has an "opaque" structure defined somewhere that is
 * stored as the page trailer.	an access method should always
 * initialize its pages with PageInit and then set its own opaque
 * fields.
 */

typedef Pointer Page;


/*
 * location (byte offset) within a page.
 *
 * note that this is actually limited to 2^15 because we have limited
 * ItemIdData.lp_off and ItemIdData.lp_len to 15 bits (see itemid.h).
 */
typedef uint16 LocationIndex;

#define PageIsValid(page) PointerIsValid(page)

#define PageGetIndex(page, index, tupsize) \
( \
	AssertMacro(PageIsValid(page)), \
	(Item)(((char *)(page)) + index * tupsize) \
)


/* ----------------------------------------------------------------
 *		extern declarations
 * ----------------------------------------------------------------
 */

extern void PageInit(Page page, Size pageSize, Size specialSize);
extern Page PageGetTempPage(Page page);
extern Page PageGetTempPageCopy(Page page);
extern void PageRestoreTempPage(Page tempPage, Page oldPage);

#endif   /* BUFPAGE_H */

// Disabled definitions

typedef struct PageHeaderData
{
	/* XXX LSN is member of *any* block, not only page-organized ones */
	XLogRecPtr	pd_lsn;			/* LSN: next byte after last byte of xlog
								 * record for last change to this page */
	uint16		pd_tli;			/* least significant bits of the TimeLineID
								 * containing the LSN */
	uint16		pd_flags;		/* flag bits, see below */
	LocationIndex pd_lower;		/* offset to start of free space */
	LocationIndex pd_upper;		/* offset to end of free space */
	LocationIndex pd_special;	/* offset to start of special space */
	uint16		pd_pagesize_version;
	TransactionId pd_prune_xid; /* oldest prunable XID, or zero if none */
	ItemIdData	pd_linp[1];		/* beginning of line pointer array */
} PageHeaderData;

#define PD_HAS_FREE_LINES	0x0001		/* are there any unused line pointers? */
#define PD_PAGE_FULL		0x0002		/* not enough free space for new
										 * tuple? */
#define PD_ALL_VISIBLE		0x0004		/* all tuples on page are visible to
										 * everyone */

#define PD_VALID_FLAG_BITS	0x0007		/* OR of all valid pd_flags bits */

#define PG_PAGE_LAYOUT_VERSION		4

#define SizeOfPageHeaderData ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageIsEmpty ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageIsNew(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageGetItemId ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageSizeIsValid(pageSize) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageGetPageSize(page) BLCKSZ

#define PageGetPageLayoutVersion(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageSetPageSizeAndVersion(page, size, version) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageGetSpecialSize(page) 0

#define PageGetSpecialPointer(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageGetItem(page, itemId) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageGetMaxOffsetNumber(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageGetLSN(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageSetLSN(page, lsn)  ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageGetTLI(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageSetTLI(page, tli) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageHasFreeLinePointers(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageSetHasFreeLinePointers(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageClearHasFreeLinePointers(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageIsFull(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageSetFull(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageClearFull(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageIsAllVisible(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageSetAllVisible(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageClearAllVisible(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageIsPrunable(page, oldestxmin) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageSetPrunable(page, xid) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

#define PageClearPrunable(page) ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("We're read only now, bitch!"))))

extern OffsetNumber PageAddItem(Page page, Item item, Size size,
			OffsetNumber offsetNumber, bool overwrite, bool is_heap);
extern Page PageGetTempPageCopySpecial(Page page);
extern void PageRepairFragmentation(Page page);
extern Size PageGetFreeSpace(Page page);
extern Size PageGetExactFreeSpace(Page page);
extern Size PageGetHeapFreeSpace(Page page);
extern void PageIndexTupleDelete(Page page, OffsetNumber offset);
extern void PageIndexMultiDelete(Page page, OffsetNumber *itemnos, int nitems);

#endif   /* BUFPAGE_H */