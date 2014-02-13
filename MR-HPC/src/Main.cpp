/*
 * Main.c
 *
 *  Created on: Feb 11, 2014
 *      Author: chung
 */

#include "mrhpc.h"

// Example of PageRank computation

/**
 * Mapper class
 */
class PR_MAP: public Mapper {
public:
	virtual void Map(const string &key, const string &value){
		int edge = 0;
		stringstream countEdge(value);
		while (countEdge.good()){
			string tmp;
			countEdge >> tmp;
			edge++;
		}

		if (edge > 1) {
			stringstream emit(value);
			int count = 0;
			while (emit.good()) {
				string tmp;
				emit >> tmp;
				if (count > 0 && count < (edge - 1)) {
					Emit(tmp, LIB::convertInt(edge - 2));
				}
				count++;
			}
		}
	}
};

/**
 * Reducer class
 */
class PR_REDUCE: public Reducer{
public:
	virtual void Reduce(const string &key, vector<string> value){
		cout << key << ": ";
		float pr = 0;
		for (int i=0; i < value.size(); i++){
			pr += (float)1/atoi(value[i].c_str());
		}
		cout << pr << "\n";
	}
};

int main(int argc, char *argv[]) {
	// Run JOB on COMM_WORLD communicator
	MPI::Init(argc, argv);

	// Set number of mapper and reducer
	MR_JOB pr (4, 4);

	// Set mapper and reducer functions
	PR_MAP map;
	pr.setM_Task(map);
	PR_REDUCE reduce;
	pr.setR_Task(reduce);

	// Set input data: text file and put (filename, each line) --> Mapper input
	pr.setInputFormat("*.txt");

	// Start job
	pr.startJob();

	MPI::Finalize();
	return 0;
}
