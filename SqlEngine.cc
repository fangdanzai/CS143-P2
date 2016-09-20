/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <limits.h>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
	fprintf(stdout, "Bruinbase> ");
	// set the command line input and start parsing user input
	sqlin = commandline;
	sqlparse(); // sqlparse() is defined in SqlParser.tab.c generated from
	// SqlParser.y by bison (bison is GNU equivalent of yacc)

	return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)//OK
{
	RecordFile rf;   // RecordFile containing the table
	RecordId   rid;  // record cursor for table scanning
	BTreeIndex index;
	vector<SelCond> condVec;

	RC     rc;
	int    key;
	string value;
	int    count;
	int    diff;

	// open the table file
	if ((rc = rf.open(table + ".tbl", 'r')) < 0)
	{
		fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
		return rc;
	}

	//check the index file
	if (index.open(table + ".idx", 'r') == 0)
	{
		count = 0;
		int keyMin = INT_MIN, keyMax = INT_MAX;
		// check boundary condition
		bool euqlasMin = false, equalsMax = false;
		vector<int> NElist;
		for (unsigned int i = 0; i < cond.size(); i++)
		{
			if (cond[i].attr == 2) condVec.push_back(cond[i]);
			else
			{
				int val = atoi(cond[i].value);
				if (cond[i].comp == SelCond::EQ)
				{
					if (val > keyMin)
					{
						keyMin = val;
						euqlasMin = true;
					}
					if (val < keyMax)
					{
						keyMax = val;
						equalsMax = true;
					}
				}
				else if (cond[i].comp == SelCond::GT)
				{
					if (val > keyMin || (val == keyMin && euqlasMin))
					{
						keyMin = val;
						euqlasMin = false;
					}
				}
				else if (cond[i].comp == SelCond::NE)
				{
					NElist.push_back(val);
				}
				else if (cond[i].comp == SelCond::LE)
				{
					if (val < keyMax)
					{
						keyMax = val;
						equalsMax = true;
					}
				}
				else if (cond[i].comp == SelCond::LT)
				{
					if (val < keyMax || (val == keyMax && euqlasMin))
					{
						keyMax = val;
						equalsMax = false;
					}
				}
				else if (cond[i].comp == SelCond::GE)
				{
					if (val > keyMin)
					{
						keyMin = val;
						euqlasMin = true;
					}
				}
			}
		}
		if (keyMin > keyMax || (keyMax == keyMin && !(euqlasMin && equalsMax)))
		{
			rf.close();
			return rc;
		}
		IndexCursor startCursor, endCursor, tempCursor, curCursor;
		index.locate(keyMin, startCursor);
		tempCursor = startCursor;
		index.readForward(startCursor, key, rid);
		if (key > keyMin || euqlasMin) startCursor = tempCursor;

		if (!index.locate(keyMax, endCursor))
		{
			tempCursor = endCursor;
			index.readForward(endCursor, key, rid);
			if (key < keyMax || !equalsMax) endCursor = tempCursor;
		}

		curCursor = startCursor;
		while (curCursor.pid != -1 && (curCursor.pid != endCursor.pid || curCursor.eid != endCursor.eid))
		{
			index.readForward(curCursor, key, rid);
			int listIndex;
			for (listIndex = 0; listIndex<NElist.size(); listIndex++)
				if (key == NElist[listIndex])
					break;

			if (listIndex < NElist.size()) continue;
			if (condVec.empty() && (attr == 1 || attr == 4))
			{
				count++;
				if (attr == 1) fprintf(stdout, "%d\n", key);
			}
			else
			{
				if ((rc = rf.read(rid, key, value)) < 0)
				{
					fprintf(stderr, "Error: cannot read a tuple from table %s\n", table.c_str());
					rf.close();
					return rc;
				}
				if (checkKeyValue(key, value, condVec))
				{
					// the condition is met for the tuple. 
					// increase matching tuple counter
					count++;

					// print the tuple
					switch (attr)
					{
					case 1:  // SELECT key
						fprintf(stdout, "%d\n", key);
						break;
					case 2:  // SELECT value
						fprintf(stdout, "%s\n", value.c_str());
						break;
					case 3:  // SELECT *
						fprintf(stdout, "%d '%s'\n", key, value.c_str());
						break;
					}
				}
			}
		}
		
		// print matching tuple count if "select count(*)"
		if (attr == 4)
		{
			fprintf(stdout, "%d\n", count);
		}
		rf.close();
		return rc;
	}
	else
		return oldSelectFunction(attr, table, cond);
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
	/* your code here */
	//Open the table file
	RecordFile newRF;
	string curTable = table + ".tbl";
	string curIndex = table + ".idx";
	if(newRF.open(curTable, 'w'))
	{
		fprintf(stderr, "Error: file %s doesn't exist or cannot be created\n", table.c_str());
		return RC_FILE_OPEN_FAILED;
	}

	//Open the loadfile
	ifstream loadFile(loadfile.c_str());
	if (!loadFile.is_open())
	{
		fprintf(stderr, "Error: file %s doesn't exist or cannot open\n", loadfile.c_str());
		return RC_FILE_OPEN_FAILED;
	}

	BTreeIndex indexTree;
	if (index && indexTree.open(curIndex, 'w'))
	{
		fprintf(stderr, "Error: file %s doesn't exist or cannot be created\n", table.c_str());
		return RC_FILE_OPEN_FAILED;
	}

	//If the loadfile exist, read the data and append them in the new table file
	int key;
	string value;
	string nextLine;
	RecordId rId = newRF.endRid();
	while (getline(loadFile, nextLine))
	{
		//If use the following code in the while loop body above,
		//then the answer we get is bigger than the correct answer,
		//but don't know why
		//loadFile.good()

		//Append the new line
		//getline(loadFile, nextLine);
		
		parseLoadLine(nextLine, key, value);
		if(newRF.append(key, value, rId))
		{
			fprintf(stderr, "Error: cannot append the key=%d value=%s into file %s \n", 
				key, value.c_str(), table.c_str());
			return RC_FILE_WRITE_FAILED;
		}
		if (index && indexTree.insert(key, rId))
		{
			fprintf(stderr, "Error: cannot append the key=%d value=%s into B+ index tree file %s \n", key, value.c_str(), table.c_str());
			return RC_FILE_WRITE_FAILED;
		}
	}
	//Close the load file, record file and B+ tree index file.
	loadFile.close();
	newRF.close();
	if (index) indexTree.close();
	return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
	const char* s;
	char c;
	string::size_type loc;

	// ignore beginning white spaces
	c = *(s = line.c_str());
	while (c == ' ' || c == '\t')
	{
		c = *++s;
	}

	// get the integer key value
	key = atoi(s);

	// look for comma
	s = strchr(s, ',');
	if (s == NULL)
	{
		return RC_INVALID_FILE_FORMAT;
	}

	// ignore white spaces
	do
	{
		c = *++s;
	}
	while (c == ' ' || c == '\t');

	// if there is nothing left, set the value to empty string
	if (c == 0)
	{
		value.erase();
		return 0;
	}

	// is the value field delimited by ' or "?
	if (c == '\'' || c == '"')
	{
		s++;
	}
	else
	{
		c = '\n';
	}

	// get the value string
	value.assign(s);
	loc = value.find(c, 0);
	if (loc != string::npos)
	{
		value.erase(loc);
	}

	return 0;
}

bool SqlEngine::checkKeyValue(const int key, const std::string& value, const std::vector<SelCond>& cond)
{
	int diff = 0;
	// check the conditions on the tuple
	for (unsigned i = 0; i < cond.size(); i++)
	{
		// compute the difference between the tuple value and the condition value
		switch (cond[i].attr)
		{
		case 1:
			diff = key - atoi(cond[i].value);
			break;
		case 2:
			diff = strcmp(value.c_str(), cond[i].value);
			break;
		}

		// skip the tuple if any condition is not met
		switch (cond[i].comp)
		{
		case SelCond::EQ:
			if (diff != 0) return false;
			break;
		case SelCond::NE:
			if (diff == 0) return false;
			break;
		case SelCond::GT:
			if (diff <= 0) return false;
			break;
		case SelCond::LT:
			if (diff >= 0) return false;
			break;
		case SelCond::GE:
			if (diff < 0) return false;
			break;
		case SelCond::LE:
			if (diff > 0) return false;
			break;
		}
	}
	return true;
}

RC SqlEngine::oldSelectFunction(int attr, const std::string& table, const std::vector<SelCond>& cond)
{
	RecordFile rf;   // RecordFile containing the table
	RecordId   rid;  // record cursor for table scanning

	RC     rc;
	int    key;
	string value;
	int    count;
	int    diff;

	// open the table file
	if ((rc = rf.open(table + ".tbl", 'r')) < 0)
	{
		fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
		return rc;
	}

	// scan the table file from the beginning
	rid.pid = rid.sid = 0;
	count = 0;
	while (rid < rf.endRid())
	{
		// read the tuple
		if ((rc = rf.read(rid, key, value)) < 0)
		{
			fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
			goto exit_select;
		}

		// check the conditions on the tuple
		for (unsigned i = 0; i < cond.size(); i++)
		{
			// compute the difference between the tuple value and the condition value
			switch (cond[i].attr)
			{
			case 1:
				diff = key - atoi(cond[i].value);
				break;
			case 2:
				diff = strcmp(value.c_str(), cond[i].value);
				break;
			}

			// skip the tuple if any condition is not met
			switch (cond[i].comp)
			{
			case SelCond::EQ:
				if (diff != 0) goto next_tuple;
				break;
			case SelCond::NE:
				if (diff == 0) goto next_tuple;
				break;
			case SelCond::GT:
				if (diff <= 0) goto next_tuple;
				break;
			case SelCond::LT:
				if (diff >= 0) goto next_tuple;
				break;
			case SelCond::GE:
				if (diff < 0) goto next_tuple;
				break;
			case SelCond::LE:
				if (diff > 0) goto next_tuple;
				break;
			}
		}

		// the condition is met for the tuple. 
		// increase matching tuple counter
		count++;

		// print the tuple 
		switch (attr)
		{
		case 1:  // SELECT key
			fprintf(stdout, "%d\n", key);
			break;
		case 2:  // SELECT value
			fprintf(stdout, "%s\n", value.c_str());
			break;
		case 3:  // SELECT *
			fprintf(stdout, "%d '%s'\n", key, value.c_str());
			break;
		}
		
	// move to the next tuple
	next_tuple:
		++rid;
	}

	// print matching tuple count if "select count(*)"
	if (attr == 4)
	{
		fprintf(stdout, "%d\n", count);
	}
	rc = 0;

// close the table file and return
exit_select:
	rf.close();
	return rc;
}
