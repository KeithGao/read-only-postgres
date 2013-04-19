/*-------------------------------------------------------------------------
 *
 * bufpage.c
 *	  POSTGRES standard buffer page code.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/page/bufpage.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/htup.h"


/* ----------------------------------------------------------------
 *						Page support functions
 * ----------------------------------------------------------------
 */

/*
 * PageInit
 *		Initializes the contents of a page.
 */
void
PageInit(Page page)
{
	PageHeader	p = (PageHeader) page;
	MemSet(p, 0, BLCKSZ);
}

/*
 * PageGetTempPage
 *		Get a temporary page in local memory for special processing.
 *		The returned page is not initialized at all; caller must do that.
 */
Page
PageGetTempPage(Page page)
{
	Page		temp;
	temp = (Page) palloc(BLCKSZ);
	return temp;
}

/*
 * PageGetTempPageCopy
 *		Get a temporary page in local memory for special processing.
 *		The page is initialized by copying the contents of the given page.
 */
Page
PageGetTempPageCopy(Page page)
{
	Page		temp;
	temp = (Page) palloc(BLCKSZ);
	memcpy(temp, page, BLCKSZ);
	return temp;
}

/*
 * PageRestoreTempPage
 *		Copy temporary page back to permanent page after special processing
 *		and release the temporary page.
 */
void
PageRestoreTempPage(Page tempPage, Page oldPage)
{
	memcpy((char *) oldPage, (char *) tempPage, BLCKSZ);
	pfree(tempPage);
}


// Disabled commands
OffsetNumber
PageAddItem(Page page,
      Item item,
      Size size,
      OffsetNumber offsetNumber,
      bool overwrite,
      bool is_heap)
{
	ereport(ERROR,
		(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		 errmsg("We're read only now, bitch!")))
}

void PageRepairFragmentation(Page page) {
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("We're read only now, bitch!")))
}

Size PageGetFreeSpace(Page page) {
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("We're read only now, bitch!")))
}

Size PageGetExactFreeSpace(Page page) {
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("We're read only now, bitch!")))
}

Size PageGetHeapFreeSpace(Page page) {
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("We're read only now, bitch!")))
}

void PageIndexTupleDelete(Page page, OffsetNumber offnum) {
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("We're read only now, bitch!")))
}

void PageIndexMultiDelete(Page page, OffsetNumber *itemnos, int nitems) {
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("We're read only now, bitch!")))
}
