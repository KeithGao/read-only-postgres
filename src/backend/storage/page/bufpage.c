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
PageInit(Page page, Size pageSize, Size specialSize)
{
	elog(DEBUG4, "Starting page init");
	NewPageHeader	p = (NewPageHeader) page;

	specialSize = MAXALIGN(specialSize);

	Assert(pageSize == BLCKSZ);
	Assert(pageSize > specialSize + SizeOfPageHeaderData);

	/* Make sure all fields of page are zero, as well as unused space */
	MemSet(p, 0, pageSize);

	/* p->pd_flags = 0;								done by above MemSet */
	p->pd_lower = SizeOfPageHeaderData;
	p->pd_upper = pageSize;
	PageSetPageSizeAndVersion(page, pageSize, PG_PAGE_LAYOUT_VERSION);
	/* p->pd_prune_xid = InvalidTransactionId;		done by above MemSet */
	elog(DEBUG4, "p->lower: %u, p->upper: %u", p->pd_lower, p->pd_upper);
}


/*
 * PageHeaderIsValid
 *		Check that the header fields of a page appear valid.
 *
 * This is called when a page has just been read in from disk.	The idea is
 * to cheaply detect trashed pages before we go nuts following bogus item
 * pointers, testing invalid transaction identifiers, etc.
 *
 * It turns out to be necessary to allow zeroed pages here too.  Even though
 * this routine is *not* called when deliberately adding a page to a relation,
 * there are scenarios in which a zeroed page might be found in a table.
 * (Example: a backend extends a relation, then crashes before it can write
 * any WAL entry about the new page.  The kernel will already have the
 * zeroed page in the file, and it will stay that way after restart.)  So we
 * allow zeroed pages here, and are careful that the page access macros
 * treat such a page as empty and without free space.  Eventually, VACUUM
 * will clean up such a page and make it usable.
 */
bool
PageHeaderIsValid(NewPageHeader page)
{
	char	   *pagebytes;
	int			i;

	/* Check normal case */
	if (PageGetPageSize(page) == BLCKSZ &&
		PageGetPageLayoutVersion(page) == PG_PAGE_LAYOUT_VERSION &&
		page->pd_lower >= SizeOfPageHeaderData &&
		page->pd_lower <= page->pd_upper)
		return true;

	/* Check all-zeroes case */
	pagebytes = (char *) page;
	for (i = 0; i < BLCKSZ; i++)
	{
		if (pagebytes[i] != 0)
			return false;
	}
	return true;
}


/*
 *	PageAddItem
 *
 *	Add an item to a page.	Return value is offset at which it was
 *	inserted, or InvalidOffsetNumber if there's not room to insert.
 *
 *	If overwrite is true, we just store the item at the specified
 *	offsetNumber (which must be either a currently-unused item pointer,
 *	or one past the last existing item).  Otherwise,
 *	if offsetNumber is valid and <= current max offset in the page,
 *	insert item into the array at that position by shuffling ItemId's
 *	down to make room.
 *	If offsetNumber is not valid, then assign one by finding the first
 *	one that is both unused and deallocated.
 *
 *	If is_heap is true, we enforce that there can't be more than
 *	MaxHeapTuplesPerPage line pointers on the page.
 *
 *	!!! EREPORT(ERROR) IS DISALLOWED HERE !!!
 */
OffsetNumber
PageAddItem(Page page,
			Item item,
			Size size,
			OffsetNumber offsetNumber,
			bool overwrite,
			bool is_heap)
{
	NewPageHeader	phdr = (NewPageHeader) page;
	Size		alignedSize;
	int			lower;
	int			upper;
	ItemId		itemId;
	OffsetNumber limit;
	bool		needshuffle = false;

	elog(DEBUG4, "In page add item");

	/*
	 * Be wary about corrupted page pointers
	 */

	if (phdr->pd_lower < SizeOfPageHeaderData ||
		phdr->pd_lower > phdr->pd_upper)
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u",
						phdr->pd_lower, phdr->pd_upper, 0)));

	/*
	 * Select offsetNumber to place the new item at
	 */
	limit = OffsetNumberNext(PageGetMaxOffsetNumber(page));

	/* was offsetNumber passed in? */
	if (OffsetNumberIsValid(offsetNumber)) {
		ereport(PANIC, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Can't modify item: read only.")));
		return InvalidOffsetNumber;
	}

	if (is_heap && offsetNumber > TUPLESIZE * MaxHeapTuplesPerPage)
	{
		elog(DEBUG4, "offset: %d, maxtuples: %d", offsetNumber, MaxHeapTuplesPerPage);
		elog(WARNING, "can't put more than MaxHeapTuplesPerPage items in a heap page");
		return InvalidOffsetNumber;
	}

	elog(DEBUG4, "computing new uppers and lowers");
	/*
	 * Compute new lower and upper pointers for page, see if it'll fit.
	 *
	 * Note: do arithmetic as signed ints, to avoid mistakes if, say,
	 * alignedSize > pd_upper.
	 */
	lower = phdr->pd_lower;

	// alignedSize = MAXALIGN(size);
	int oldUpper = phdr->pd_upper;
	upper = (int) phdr->pd_upper - TUPLESIZE;

	if (lower > upper) {
		elog(DEBUG4, "lower > upper");
		return limit;
	}
	elog(DEBUG4, "copying upper %d, size %d, blocksize %d", upper, TUPLESIZE, BLCKSZ);
	/* copy the item's data onto the page */
	memcpy((char *) page + upper, item, TUPLESIZE);
	/* adjust page header */
	// phdr->pd_lower = (LocationIndex) lower;
	phdr->pd_upper = (LocationIndex) upper;
	elog(DEBUG4, "Done with my page add");
	return oldUpper ;
}

/*
 * PageGetTempPage
 *		Get a temporary page in local memory for special processing.
 *		The returned page is not initialized at all; caller must do that.
 */
Page
PageGetTempPage(Page page)
{
	Size		pageSize;
	Page		temp;

	pageSize = PageGetPageSize(page);
	temp = (Page) palloc(pageSize);

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
	Size		pageSize;
	Page		temp;

	pageSize = PageGetPageSize(page);
	temp = (Page) palloc(pageSize);

	memcpy(temp, page, pageSize);

	return temp;
}

/*
 * PageGetTempPageCopySpecial
 *		Get a temporary page in local memory for special processing.
 *		The page is PageInit'd with the same special-space size as the
 *		given page, and the special space is copied from the given page.
 */
Page
PageGetTempPageCopySpecial(Page page)
{
	Size		pageSize;
	Page		temp;

	pageSize = PageGetPageSize(page);
	temp = (Page) palloc(pageSize);

	PageInit(temp, pageSize, PageGetSpecialSize(page));
	memcpy(PageGetSpecialPointer(temp),
		   PageGetSpecialPointer(page),
		   PageGetSpecialSize(page));

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
	Size		pageSize;

	pageSize = PageGetPageSize(tempPage);
	memcpy((char *) oldPage, (char *) tempPage, pageSize);

	pfree(tempPage);
}

/*
 * sorting support for PageRepairFragmentation and PageIndexMultiDelete
 */
typedef struct itemIdSortData
{
	int			offsetindex;	/* linp array index */
	int			itemoff;		/* page offset of item data */
	Size		alignedlen;		/* MAXALIGN(item data len) */
	ItemIdData	olditemid;		/* used only in PageIndexMultiDelete */
} itemIdSortData;
typedef itemIdSortData *itemIdSort;

static int
itemoffcompare(const void *itemidp1, const void *itemidp2)
{
	/* Sort in decreasing itemoff order */
	return ((itemIdSort) itemidp2)->itemoff -
		((itemIdSort) itemidp1)->itemoff;
}

/*
 * PageRepairFragmentation
 *
 * Frees fragmented space on a page.
 * It doesn't remove unused line pointers! Please don't change this.
 *
 * This routine is usable for heap pages only, but see PageIndexMultiDelete.
 *
 * As a side effect, the page's PD_HAS_FREE_LINES hint bit is updated.
 */
void
PageRepairFragmentation(Page page)
{
	Offset		pd_lower = ((PageHeader) page)->pd_lower;
	Offset		pd_upper = ((PageHeader) page)->pd_upper;
	Offset		pd_special = ((PageHeader) page)->pd_special;
	itemIdSort	itemidbase,
				itemidptr;
	ItemId		lp;
	int			nline,
				nstorage,
				nunused;
	int			i;
	Size		totallen;
	Offset		upper;

	/*
	 * It's worth the trouble to be more paranoid here than in most places,
	 * because we are about to reshuffle data in (what is usually) a shared
	 * disk buffer.  If we aren't careful then corrupted pointers, lengths,
	 * etc could cause us to clobber adjacent disk buffers, spreading the data
	 * loss further.  So, check everything.
	 */
	if (pd_lower < SizeOfPageHeaderData ||
		pd_lower > pd_upper ||
		pd_upper > pd_special ||
		pd_special > BLCKSZ ||
		pd_special != MAXALIGN(pd_special))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("corrupted page pointers: lower = %u, upper = %u, special = %u",
						pd_lower, pd_upper, pd_special)));

	nline = PageGetMaxOffsetNumber(page);
	nunused = nstorage = 0;
	for (i = FirstOffsetNumber; i <= nline; i++)
	{
		lp = PageGetItemId(page, i);
		if (ItemIdIsUsed(lp))
		{
			if (ItemIdHasStorage(lp))
				nstorage++;
		}
		else
		{
			/* Unused entries should have lp_len = 0, but make sure */
			ItemIdSetUnused(lp);
			nunused++;
		}
	}

	if (nstorage == 0)
	{
		/* Page is completely empty, so just reset it quickly */
		((PageHeader) page)->pd_upper = pd_special;
	}
	else
	{							/* nstorage != 0 */
		/* Need to compact the page the hard way */
		itemidbase = (itemIdSort) palloc(sizeof(itemIdSortData) * nstorage);
		itemidptr = itemidbase;
		totallen = 0;
		for (i = 0; i < nline; i++)
		{
			lp = PageGetItemId(page, i + 1);
			if (ItemIdHasStorage(lp))
			{
				itemidptr->offsetindex = i;
				itemidptr->itemoff = ItemIdGetOffset(lp);
				if (itemidptr->itemoff < (int) pd_upper ||
					itemidptr->itemoff >= (int) pd_special)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("corrupted item pointer: %u",
									itemidptr->itemoff)));
				itemidptr->alignedlen = MAXALIGN(ItemIdGetLength(lp));
				totallen += itemidptr->alignedlen;
				itemidptr++;
			}
		}

		if (totallen > (Size) (pd_special - pd_lower))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
			   errmsg("corrupted item lengths: total %u, available space %u",
					  (unsigned int) totallen, pd_special - pd_lower)));

		/* sort itemIdSortData array into decreasing itemoff order */
		qsort((char *) itemidbase, nstorage, sizeof(itemIdSortData),
			  itemoffcompare);

		/* compactify page */
		upper = pd_special;

		for (i = 0, itemidptr = itemidbase; i < nstorage; i++, itemidptr++)
		{
			lp = PageGetItemId(page, itemidptr->offsetindex + 1);
			upper -= itemidptr->alignedlen;
			memmove((char *) page + upper,
					(char *) page + itemidptr->itemoff,
					itemidptr->alignedlen);
			lp->lp_off = upper;
		}

		((PageHeader) page)->pd_upper = upper;

		pfree(itemidbase);
	}

	/* Set hint bit for PageAddItem */
	if (nunused > 0)
		PageSetHasFreeLinePointers(page);
	else
		PageClearHasFreeLinePointers(page);
}

/*
 * PageGetFreeSpace
 *		Returns the size of the free (allocatable) space on a page,
 *		reduced by the space needed for a new line pointer.
 *
 * Note: this should usually only be used on index pages.  Use
 * PageGetHeapFreeSpace on heap pages.
 */
Size
PageGetFreeSpace(Page page)
{
	int			space;

	/*
	 * Use signed arithmetic here so that we behave sensibly if pd_lower >
	 * pd_upper.
	 */
	space = (int) ((NewPageHeader) page)->pd_upper -
		(int) ((NewPageHeader) page)->pd_lower;

	if (space < (int) sizeof(ItemIdData))
		return 0;
	space -= sizeof(ItemIdData);

	return (Size) space;
}

/*
 * PageGetExactFreeSpace
 *		Returns the size of the free (allocatable) space on a page,
 *		without any consideration for adding/removing line pointers.
 */
Size
PageGetExactFreeSpace(Page page)
{
	int			space;

	/*
	 * Use signed arithmetic here so that we behave sensibly if pd_lower >
	 * pd_upper.
	 */
	space = (int) ((NewPageHeader) page)->pd_upper -
		(int) ((NewPageHeader) page)->pd_lower;

	if (space < 0)
		return 0;

	return (Size) space;
}


/*
 * PageGetHeapFreeSpace
 *		Returns the size of the free (allocatable) space on a page,
 *		reduced by the space needed for a new line pointer.
 *
 * The difference between this and PageGetFreeSpace is that this will return
 * zero if there are already MaxHeapTuplesPerPage line pointers in the page
 * and none are free.  We use this to enforce that no more than
 * MaxHeapTuplesPerPage line pointers are created on a heap page.  (Although
 * no more tuples than that could fit anyway, in the presence of redirected
 * or dead line pointers it'd be possible to have too many line pointers.
 * To avoid breaking code that assumes MaxHeapTuplesPerPage is a hard limit
 * on the number of line pointers, we make this extra check.)
 */
Size
PageGetHeapFreeSpace(Page page)
{
	Size		space;

	space = PageGetFreeSpace(page);
	if (space > 0)
	{
		OffsetNumber offnum,
					nline;

		/*
		 * Are there already MaxHeapTuplesPerPage line pointers in the page?
		 */
		nline = PageGetMaxOffsetNumber(page);
		if (nline >= MaxHeapTuplesPerPage)
		{
			if (PageHasFreeLinePointers((NewPageHeader) page))
			{
				/*
				 * Since this is just a hint, we must confirm that there is
				 * indeed a free line pointer
				 */
				for (offnum = FirstOffsetNumber; offnum <= nline; offnum = OffsetNumberNext(offnum))
				{
					ItemId		lp = PageGetItemId(page, offnum);

					if (!ItemIdIsUsed(lp))
						break;
				}

				if (offnum > nline)
				{
					/*
					 * The hint is wrong, but we can't clear it here since we
					 * don't have the ability to mark the page dirty.
					 */
					space = 0;
				}
			}
			else
			{
				/*
				 * Although the hint might be wrong, PageAddItem will believe
				 * it anyway, so we must believe it too.
				 */
				space = 0;
			}
		}
	}
	return space;
}


/*
 * PageIndexTupleDelete
 *
 * This routine does the work of removing a tuple from an index page.
 *
 * Unlike heap pages, we compact out the line pointer for the removed tuple.
 */
void
PageIndexTupleDelete(Page page, OffsetNumber offnum)
{
	ereport(PANIC, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("No delete: read only now, bitch!")));
}


/*
 * PageIndexMultiDelete
 *
 * This routine handles the case of deleting multiple tuples from an
 * index page at once.	It is considerably faster than a loop around
 * PageIndexTupleDelete ... however, the caller *must* supply the array
 * of item numbers to be deleted in item number order!
 */
void
PageIndexMultiDelete(Page page, OffsetNumber *itemnos, int nitems)
{
	ereport(PANIC, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("No delete: read only now, bitch!")));
}
