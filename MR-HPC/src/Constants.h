/*
 * Constants.h
 *
 *  Created on: Feb 6, 2014
 *      Author: chung
 */
#ifndef CONSTANTS_H_
#define CONSTANTS_H_

class TAG{
	public:
    	static const int MAP_SEND = 1;
    	static const int REDUCE_RECV = 1;
    	static const int TASK_END = -1;
    	static const char EXIT	= '0';
    	static const char SPLIT	= '@';
};

class MR{
	public:
		static const int MAX_DATA_IN_MSG = 1000;
		static const int MAX_MAPPER = 1000;
		static const int MAX_REDUCER = 1000;
};

#endif /* CONSTANTS_H_ */
