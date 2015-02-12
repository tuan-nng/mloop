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


#include <stdio.h>

#define DEFAULT_BUFF_SIZE 64 * 1024
using std::string;

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
public:
    LineReader(hdfsFS fs, hdfsFile file);
    bool backfill();
    int readLine(string& str,int maxLineLength, int maxBytesToConsume);
    int readLine(string& str,int maxLineLength);
    int readLine(string& str);
    void close();
    ~LineReader();
};

#endif	/* LINEREADER_HPP */

