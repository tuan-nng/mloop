/* 
 * File:   LineReader.hpp
 * Author: novpla
 *
 * Created on February 12, 2015, 11:52 AM
 */

#ifndef LINEREADER_HPP
#define	LINEREADER_HPP

#include <string>
#include "libhdfs/hdfs.h"
#include <vector>


#include <stdio.h>

using namespace std;

class LineReader {
private:
    int bufferSize;
    char* buffer;
    // the number of bytes of real data in the buffer
    int bufferLen;
    // the current position in the buffer
    int bufferPosn;
    hdfsFS fs;
    hdfsFile file;
    string bufferStr;
public:
    LineReader(hdfsFS fs, hdfsFile file);
    bool backfill();
    int readLine(char* &str,int maxLineLength, int maxBytesToConsume);
    int readLine(char* &str,int maxLineLength);
    int readLine(char* &str);
    void close();
    ~LineReader();
};

#endif	/* LINEREADER_HPP */

