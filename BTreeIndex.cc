/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
#include <stdio.h> 
#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	if (pf.open(indexname, mode)) return RC_FILE_OPEN_FAILED;
	char buffer[PageFile::PAGE_SIZE];
	//Open the new file
	//If cannot read the file or write the file, return error code -2
	if (!pf.endPid())
	{
		*(PageId*)(buffer + sizeof(int)) = rootPid = -1;
		*(int*)buffer = treeHeight = 0;
		if (pf.write(0, buffer)) return RC_FILE_WRITE_FAILED;
	}
	//If exist the current file, read data from PageId = 0.
	if (pf.read(0, buffer)) return RC_FILE_READ_FAILED;
	rootPid = *(PageId*)(buffer + sizeof(int));
	treeHeight = *(int*)buffer;
	isWrite = false;
	if (mode == 'w') isWrite = true;
	return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	if (isWrite)
	{
		char buffer[PageFile::PAGE_SIZE];
		*((int*)buffer) = treeHeight;
		*((PageId*)(buffer + sizeof(int))) = rootPid;
		if (pf.write(0, buffer)) return RC_FILE_WRITE_FAILED;
	}
	return pf.close();
    //return 0;
}

bool BTreeIndex::insertRecursive(int key, const RecordId& rid, PageId pid, int height, int& newKey, PageId& pageID)
{
	if (height >= treeHeight)
	{
		BTLeafNode leafnode;
		if (leafnode.read(pid, pf)) return RC_FILE_READ_FAILED;
		if (leafnode.getKeyCount() < BTLeafNode::MAX_KEY_NUMBER)
		{
			leafnode.insert(key, rid);
			leafnode.write(pid, pf);
			return false;
		}
		else
		{
			BTLeafNode newNode;
			leafnode.insertAndSplit(key, rid, newNode, newKey);
			leafnode.setNextNodePtr(pf.endPid());
			leafnode.write(pid, pf);
			pageID = pf.endPid();
			newNode.write(pf.endPid(), pf);
			return true;
		}
	}
	else
	{
		BTNonLeafNode nonleaf;
		if (nonleaf.read(pid, pf)) return true;
		PageId child;
		nonleaf.locateChildPtr(key, child);
		if (insertRecursive(key, rid, child, height + 1, newKey, pageID))
		{
			if (nonleaf.getKeyCount() < BTNonLeafNode::MAX_KEY_NUMBER)
			{
				nonleaf.insert(newKey, pageID);
				nonleaf.write(pid, pf);
				return false;
			}
			else
			{
				BTNonLeafNode newNode;
				nonleaf.insertAndSplit(newKey, pageID, newNode, newKey);
				nonleaf.write(pid, pf);
				pageID = pf.endPid();
				newNode.write(pf.endPid(), pf);
				return true;
			}
		}
		return false;
	}
}


/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	int newKey;
	PageId pageID;
	if (rootPid==-1)
	{
		BTLeafNode leafNode;
		leafNode.insert(key, rid);
		rootPid = pf.endPid();
		treeHeight = 1;
		leafNode.write(rootPid, pf);
		return 0;
	}
	int height = 1;
	if (insertRecursive(key, rid, rootPid, height, newKey, pageID))
	{
		BTNonLeafNode nonLeaf;
		nonLeaf.initializeRoot(rootPid, newKey, pageID);
		rootPid = pf.endPid();
		treeHeight++;
		nonLeaf.write(rootPid, pf);
	}
	return 0;
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf nonleaf where searchKey may exist. If an index entry with
 * searchKey exists in the leaf nonleaf, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf nonleaf, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf nonleaf and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	if (rootPid == -1) return RC_NO_SUCH_RECORD;
	PageId pageID = rootPid;
	for (int i = 1; i < treeHeight;i++)
	{
		BTNonLeafNode nonLeaf;
		nonLeaf.read(pageID, pf);
		nonLeaf.locateChildPtr(searchKey, pageID);
	}
	BTLeafNode leafNode;
	leafNode.read(pageID, pf);
	cursor.pid = pageID;
	return leafNode.locate(searchKey, cursor.eid);
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-nonleaf index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	BTLeafNode leafNode;
	if (leafNode.read(cursor.pid, pf)) return RC_FILE_READ_FAILED;
	leafNode.readEntry(cursor.eid, key, rid);
	if (cursor.eid >= leafNode.getKeyCount() - 1)
	{
		cursor.pid = leafNode.getNextNodePtr();
		cursor.eid = 0;
	}
	else
		cursor.eid++;
	return 0;
}
