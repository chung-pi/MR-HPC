/*
 * mrhpc.cpp
 *
 *  Created on: Feb 11, 2014
 *      Author: chung
 */
#include "mrhpc.h"

void Mapper::initialize(int rNodeNumber, int *rNodeList){
	this->rNodeNumber = rNodeNumber;
	this->rNodeList = rNodeList;
}

void Mapper::Emit(const string &key, const string &value){
	unsigned long long int hash = LIB::getHash(key);
	string keyValue = key + TAG::SPLIT + value;
	MPI::COMM_WORLD.Send(keyValue.c_str(), (keyValue.size() + 1), MPI::CHAR, *(this->rNodeList + hash%this->rNodeNumber), TAG::MAP_SEND);
}

void Mapper::End(){
	for (int i=0; i < this->rNodeNumber; i++){
		char data = TAG::EXIT;
		MPI::COMM_WORLD.Send(&data, 1, MPI::CHAR, *(this->rNodeList + i), TAG::MAP_SEND);
	}
}

void Reducer::initialize(int mNodeNumber, string tmpDir){
	this->mNodeNumber = mNodeNumber;
	this->tmpDir = tmpDir;
}

void Reducer::wait(){
	int count = 0;
	ReduceInput list(rand(), this->tmpDir);
	for(;;){
		MPI::Status status;
		char *data = new char[MR::MAX_DATA_IN_MSG];
		MPI::COMM_WORLD.Recv(data, MR::MAX_DATA_IN_MSG, MPI::CHAR, MPI::ANY_SOURCE, TAG::REDUCE_RECV, status);
		if (status.Get_count(MPI::CHAR) == 1 && data[0] == TAG::EXIT){
			count++;
		}else{
			// split[0] => Key, split[1] => Value
			std::vector<std::string> split = LIB::split(data, TAG::SPLIT);

			// Out of core buffer implementation here
			list.addKeyValue(split[0], split[1]);
		}
		if (count == this->mNodeNumber){
			break;
		}
	}

	// Forward (key, value) to Reducer
	vector<string> listKey = list.getListKey();
	for (int i=0; i < listKey.size(); i++){
		this->Reduce(listKey[i], list.getKeyValue(listKey[i]));
	}

	/*
	vector<string> listKey;
	listKey.push_back("3");
	this->Reduce("3", listKey);
	*/
}

void Mapper::wait(){
	int length;
	MPI::Status status;
	MPI::COMM_WORLD.Recv(&length, 1, MPI::INT, MPI::ANY_SOURCE, TAG::REDUCE_RECV, status);

	char *data = new char[length];
	MPI::COMM_WORLD.Recv(data, length, MPI::CHAR, MPI::ANY_SOURCE, TAG::REDUCE_RECV, status);
	this->listInput = LIB::split(data, TAG::SPLIT);
}

vector<string> Mapper::getListInput(){
	return this->listInput;
}

MR_JOB::MR_JOB(int mNodeNumber, int rNodeNumber){
	this->mNodeNumber = mNodeNumber;
	this->rNodeNumber = rNodeNumber;
	this->map = 0;
	this->reduce = 0;
	rNodeList = new int[this->rNodeNumber];
	for (int i=0; i < this->rNodeNumber; i++){
		rNodeList[i] = i;
	}

	// Set temporary directory
	this->tmpDir = "";
}

void MR_JOB::setM_Task(Mapper &m){
	this->map = &m;
}

void MR_JOB::setR_Task(Reducer &r){
	this->reduce = &r;
}

int MR_JOB::initialize(){
	int size;
	size = MPI::COMM_WORLD.Get_size();
	if (size < this->rNodeNumber + this->mNodeNumber){
		printf("Not enough node.\n");
		return 0;
	}

	this->map->initialize(this->rNodeNumber, rNodeList);
	this->reduce->initialize(this->mNodeNumber, this->tmpDir);

	return 1;
}

void MR_JOB::readData(){
	vector<string> listFile = this->map->getListInput();

	for (int i = 0; i < listFile.size(); i++) {
		ifstream file(listFile[i].c_str());
		int count = 0;
		string line;
		while (getline(file, line)) {
			this->map->Map(listFile[i], line);
			count++;
			//std::cout << "Mapper " << MPI::COMM_WORLD.Get_rank() << ": " << listFile[i] << " " << count << "\n";
		}
		file.close();
	}
}

void MR_JOB::splitData(){
	vector<string> listFile;
	int size = this->mNodeNumber;
	vector<string> listInput[MR::MAX_MAPPER];
	int index = 0;

	int get = LIB::getDir(".", listFile);
	if (get == 0){
		for (int i=0; i < listFile.size(); i++){
			if (LIB::wildCMP(this->inputFormat.c_str(), listFile[i].c_str())){
				listInput[index].push_back(listFile[i]);
				index = (index + 1) % size;
			}
		}
	}

	// Send input data to each Mapper
	for (int i=0; i < this->mNodeNumber; i++){
		string data;
		if (listInput[i].size() > 0) {
			data = listInput[i][0];
			for (int j = 1; j < listInput[i].size(); j++) {
				data = data + TAG::SPLIT + listInput[i][j];
			}
		}
		int length = data.size() + 1;
		MPI::COMM_WORLD.Send(&length, 1, MPI::INT,
				(this->rNodeNumber + i), TAG::MAP_SEND);
		MPI::COMM_WORLD.Send(data.c_str(), length, MPI::CHAR,
				(this->rNodeNumber + i), TAG::MAP_SEND);
	}
}

void MR_JOB::startJob(){
	if (this->initialize() == 0){
		return;
	}
	int rank = MPI::COMM_WORLD.Get_rank();
	if (rank >= this->rNodeNumber){
		if (rank < (this->rNodeNumber + this->mNodeNumber)){
			if (rank == this->rNodeNumber){
				splitData();
			}

			this->map->wait();
			readData();
			//std::cout << "Mapper " << MPI::COMM_WORLD.Get_rank() << ": Done" << "\n";
			this->map->End();
		}
	}else{
		this->reduce->wait();
		//std::cout << "Reducer " << MPI::COMM_WORLD.Get_rank() << ": Done" << "\n";
	}
}

void MR_JOB::setInputFormat(const string &format){
	this->inputFormat = format;
}

void MR_JOB::setTmpDir(const string &dir){
	this->tmpDir = dir;
}

unsigned long long int LIB::getHash(const string str){
    unsigned long long int hash = 0;
    for(size_t i = 0; i < str.length(); ++i)
        hash = 65599 * hash + str[i];
    return hash;
}

void LIB::trim(string& str){
	string::size_type pos = str.find_last_not_of(' ');
	if (pos != string::npos) {
		str.erase(pos + 1);
		pos = str.find_first_not_of(' ');
		if (pos != string::npos)
			str.erase(0, pos);
	} else
		str.erase(str.begin(), str.end());
}

std::vector<std::string> &LIB::split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}


std::vector<std::string> LIB::split(const std::string &s, char delim) {
    std::vector<std::string> elems;
    LIB::split(s, delim, elems);
    return elems;
}

string LIB::convertInt(int number){
   stringstream ss;
   ss << number;
   return ss.str();
}

bool LIB::fileExist(const std::string name) {
	ifstream file(name.c_str());
	if (!file){
		return false;
	}else{
		file.close();
		return true;
	}
}

int LIB::getDir(string dir, vector<string> &files){
    DIR *dp;
    struct dirent *dirp;
    if((dp  = opendir(dir.c_str())) == NULL) {
        cout << "Error(" << errno << ") opening " << dir << endl;
        return errno;
    }

    while ((dirp = readdir(dp)) != NULL) {
        files.push_back(string(dirp->d_name));
    }
    closedir(dp);
    return 0;
}

int LIB::wildCMP(const char *wild, const char *string) {
	const char *cp = NULL, *mp = NULL;

	while ((*string) && (*wild != '*')) {
		if ((*wild != *string) && (*wild != '?')) {
			return 0;
		}
		wild++;
		string++;
	}

	while (*string) {
		if (*wild == '*') {
			if (!*++wild) {
				return 1;
			}
			mp = wild;
			cp = string + 1;
		} else if ((*wild == *string) || (*wild == '?')) {
			wild++;
			string++;
		} else {
			wild = mp;
			string = cp++;
		}
	}

	while (*wild == '*') {
		wild++;
	}
	return !*wild;
}

ReduceInput::ReduceInput(int jobID, string tmpDir){
	this->jobID = jobID;
	this->tmpDir = tmpDir;
}

void ReduceInput::addKeyValue(const string &key, const string &value){
	string fileName = this->tmpDir + "/" +LIB::convertInt(this->jobID) + TAG::SPLIT + key;
	if (LIB::fileExist(fileName)){
		ofstream file(fileName.c_str(), ios::app);
		file << value << "\n";
		file.close();
	}else{
		this->listKey.push_back(key);
		ofstream file(fileName.c_str());
		file << value << "\n";
		file.close();
	}
}

vector<string> ReduceInput::getKeyValue(const string &key){
	string fileName = this->tmpDir + "/" + LIB::convertInt(this->jobID) + TAG::SPLIT + key;
	vector<string> listValue;
	ifstream file(fileName.c_str());
	if (file.is_open()){
		string line;
		while (getline(file, line)) {
			listValue.push_back(line);
		}
	}
	file.close();
	remove(fileName.c_str());
	return listValue;
}

vector<string> ReduceInput::getListKey(){
	return this->listKey;
}
