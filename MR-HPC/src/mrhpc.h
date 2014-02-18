/*
 * mrhpc.h
 *
 *  Created on: Feb 11, 2014
 *      Author: chung
 */

#ifndef MRHPC_H_
#define MRHPC_H_

#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdlib>
#include <dirent.h>
#include <errno.h>

#include <mpi.h>
#include "Constants.h"

using std::string;
using namespace std;

class Mapper {
	int *rNodeList;
	int rNodeNumber;
	vector<string> listInput;
public:
	virtual ~Mapper(){};
	virtual void Map(const string &key,const string &value) = 0;

	virtual void Emit(const string &key, const string &value);
	virtual void End();
	void initialize(int, int*);
	void wait();
	vector<string> getListInput();
};

class ReduceInput{
	vector<string> listKey;
	int jobID;
	string tmpDir;

public:
	ReduceInput(int jobID, string tmpDir);
	void addKeyValue(const string &key, const string &value);
	vector<string> getKeyValue(const string &key);
	virtual ~ReduceInput(){};
	vector<string> getListKey();
};

class Reducer{
	int mNodeNumber;
	string tmpDir;

public:
	virtual ~Reducer(){};
	virtual void Reduce(const string &key, vector<string> value) = 0;
	void initialize(int, string);
	void wait();

	virtual void Emit(const string &value){};
};

class MR_JOB{
	int mNodeNumber;
	int rNodeNumber;
	int *rNodeList;
	Mapper *map;
	Reducer *reduce;
	string inputFormat;
	string tmpDir;
public:
	MR_JOB(int mNodeNumber, int rNodeNumber);
	virtual ~MR_JOB(){};
	void setM_Task(Mapper &m);
	void setR_Task(Reducer &r);
	int initialize();
	void startJob();
	void setInputFormat(const string &format);
	void readData();
	void splitData();
	void setTmpDir(const string &dir);
};

class LIB{
public:
	static unsigned long long int getHash(const string str);
	static std::vector<std::string> split(const std::string &s, char delim);
	static std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems);
	static string convertInt(int number);
	static bool fileExist(const std::string name);
	static int getDir(string dir, vector<string> &files);
	static int wildCMP(const char *wild, const char *string);
	static void trim(string& str);
};

#endif /* MRHPC_H_ */
