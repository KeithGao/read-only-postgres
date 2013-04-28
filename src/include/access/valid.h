/*-------------------------------------------------------------------------
 *
 * valid.h
 *	  POSTGRES tuple qualification validity definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/valid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VALID_H
#define VALID_H

/*
 *		HeapKeyTest
 *
 *		Test a heap tuple to see if it satisfies a scan key.
 */
#define HeapKeyTest(tuple, \
					tupdesc, \
					nkeys, \
					keys, \
					result) \
do \
{ \
	elog(DEBUG4, "Performing a comparison"); \
	/* Use underscores to protect the variables passed in as parameters */ \
	int			__cur_nkeys = (nkeys); \
	ScanKey		__cur_keys = (keys); \
 \
	(result) = true; /* may change */ \
	for (; __cur_nkeys--; __cur_keys++) \
	{ \
		Datum	__atp; \
		bool	__isnull; \
		Datum	__test; \
 \
		if (__cur_keys->sk_flags & SK_ISNULL) \
		{ \
			elog(DEBUG4, "skolem is null"); \
			(result) = false; \
			break; \
		} \
		__atp = heap_getattr((tuple), \
							 __cur_keys->sk_attno, \
							 (tupdesc), \
							 &__isnull); \
		elog(DEBUG4, "Just got attribute %.5s", __atp); \
 \
		if (__isnull) \
		{ \
			elog(DEBUG4, "attr is null"); \
			(result) = false; \
			break; \
		} \
 \
 		elog(DEBUG4, "Calling function %u on tuple %p with datum %p", __cur_keys->sk_func.fn_oid, (tuple), __cur_keys->sk_argument); \
		__test = FunctionCall2Coll(&__cur_keys->sk_func, \
								   __cur_keys->sk_collation, \
								   __atp, __cur_keys->sk_argument); \
 \
		if (!DatumGetBool(__test)) \
		{ \
			(result) = false; \
			break; \
		} \
	} \
} while (0)

//  \
//  		int __tuplec; \
//  		for (__tuplec = 0; __tuplec < TUPLESIZE; __tuplec++) { \
//  			elog(DEBUG4, "Byte is %u", *((char *)(tuple) + (int)(sizeof(HeapTupleStartData) + __tuplec))); \
//  		} \
// \

#endif   /* VALID_H */
