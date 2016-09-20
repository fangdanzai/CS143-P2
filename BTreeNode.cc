#include "BTreeNode.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

using namespace std;

BTLeafNode::BTLeafNode()
{
  *(int *)buffer = 0;
  setNextNodePtr(-1);
}
/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	return pf.read(pid, buffer); 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 
  return *((int *)buffer); 
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{
 	int count = getKeyCount();
	if (MAX_KEY_NUMBER <= count) return RC_NODE_FULL;
	else
	{
		int eID, i;
		locate(key, eID);
		PageId pageID = getNextNodePtr();
		for (i = count; i > eID; --i)
		{
			int destIndex = PID_SIZE*i + sizeof(int);
			int srcIndex = (i - 1)*PID_SIZE + sizeof(int);
			memcpy(buffer + destIndex, buffer + srcIndex, PID_SIZE);
		}
		int tempIndex = PID_SIZE*eID + sizeof(int);
		*(RecordId*)(buffer + tempIndex) = rid;
		*(int*)(buffer + tempIndex + sizeof(RecordId)) = key;
		(*(int*)buffer)++;
		setNextNodePtr(pageID);
		return 0;
	}


}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, BTLeafNode& sibling, int& siblingKey)
{
	//--------------------start insert---------------------------
	int count = getKeyCount();
	int eID, i;
	locate(key, eID);
	PageId pageID = getNextNodePtr();
	for (i = count; i > eID; --i)
	{
		int destIndex = PID_SIZE*i + sizeof(int);
		int srcIndex = (i - 1)*PID_SIZE + sizeof(int);
		memcpy(buffer + destIndex, buffer + srcIndex, PID_SIZE);
	}
	int tempIndex = PID_SIZE*eID + sizeof(int);
	*(RecordId*)(buffer + tempIndex) = rid;
	*(int*)(buffer + tempIndex + sizeof(RecordId)) = key;

	//--------------------split insert---------------------------
	int lessKey = (MAX_KEY_NUMBER + 1) / 2;
	int moreKey = (MAX_KEY_NUMBER + 1) - lessKey;
	*(int*)buffer = lessKey;
	*(int*)sibling.buffer = moreKey;
	memcpy(sibling.buffer + sizeof(int), buffer + sizeof(int) + lessKey*PID_SIZE, moreKey*PID_SIZE);
	sibling.setNextNodePtr(pageID);
	siblingKey = *(int*)(sibling.buffer + sizeof(int) + sizeof(RecordId));
	return 0;


}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{
	eid = 0;
	while (eid<getKeyCount())
	{
		if (*(int*)(buffer + sizeof(int) + sizeof(RecordId) + PID_SIZE*eid) >= searchKey)
			return 0;
		eid++;
	}
	//If not found, then return no such record 
	return RC_NO_SUCH_RECORD;
	
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
 	rid = *(RecordId*)(buffer + eid*PID_SIZE + sizeof(int));
	key = *(int*)(buffer + eid*PID_SIZE + sizeof(int) + sizeof(RecordId));
	return 0;

}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{
	return *(PageId *)(buffer + sizeof(int) + getKeyCount()*PID_SIZE);
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
	*(PageId *)(buffer + sizeof(int) + getKeyCount()*PID_SIZE) = pid;
	return 0; 
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
	return pf.read(pid, buffer); 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
	return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
	return *((int *)buffer);
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
	int count = getKeyCount();
	if (MAX_KEY_NUMBER <= count) return RC_NODE_FULL;
	int i = 0, j=count;
	while (i<count)
	{
		int tempIndex = sizeof(int) + sizeof(PageId) + i*PID_SIZE;
		if (*(int*)(buffer + tempIndex) > key) break;
		i++;
	}
	while (j>i)
	{
		int destIndex = sizeof(int) + sizeof(PageId) + j*PID_SIZE;
		int srcIndex = sizeof(int) + sizeof(PageId) + (j - 1)*PID_SIZE;
		memcpy(buffer + destIndex, buffer + srcIndex, PID_SIZE);
		j--;
	}
	*(PageId*)(buffer + 2 * sizeof(int) + sizeof(PageId) + i*PID_SIZE) = pid;
	*(int*)(buffer + sizeof(int) + sizeof(PageId) + i*PID_SIZE) = key;
	(*(int*)buffer)++;
	return 0;

}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{
	//----------------------insert start----------------------------------------
	int count = getKeyCount();
	int i = 0, j = count;
	while (i<count)
	{
		int tempIndex = sizeof(int) + sizeof(PageId) + i*PID_SIZE;
		if (*(int*)(buffer + tempIndex) > key) break;
		i++;
	}
	while (j>i)
	{
		int destIndex = sizeof(int) + sizeof(PageId) + j*PID_SIZE;
		int srcIndex = sizeof(int) + sizeof(PageId) + (j - 1)*PID_SIZE;
		memcpy(buffer + destIndex, buffer + srcIndex, PID_SIZE);
		j--;
	}
	*(PageId*)(buffer + 2 * sizeof(int) + sizeof(PageId) + i*PID_SIZE) = pid;
	*(int*)(buffer + sizeof(int) + sizeof(PageId) + i*PID_SIZE) = key;

	//------------------------split start---------------------------------------------
	int lessKey = (MAX_KEY_NUMBER + 1) / 2;
	int moreKey = MAX_KEY_NUMBER - lessKey;
	*(int*)buffer = lessKey;
	*(int*)sibling.buffer = moreKey;
	memcpy(sibling.buffer + sizeof(int), buffer + sizeof(int) + (lessKey + 1)*PID_SIZE, moreKey*PID_SIZE + sizeof(PageId));
	midKey = *(int*)(sibling.buffer + sizeof(int) + sizeof(PageId));
	return 0;


}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
	int eid=0;
	while (eid<getKeyCount())
	{
		if (*(int*)(buffer + sizeof(int) + sizeof(PageId) + eid*PID_SIZE) > searchKey) break;
		eid++;
	}
	pid = *(PageId*)(buffer + sizeof(int) + eid*PID_SIZE);
	return 0;

}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
	*(int *)buffer = 1;
	*(int *)(buffer + sizeof(int) + sizeof(PageId)) = key;
	*(PageId *)(buffer + sizeof(int)) = pid1;
	*(PageId *)(buffer + sizeof(int) + PID_SIZE) = pid2;
	return 0;

}
