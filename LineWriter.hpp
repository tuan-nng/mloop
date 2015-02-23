/* 
 * File:   LineWriter.hpp
 * Author: novpla
 *
 * Created on February 13, 2015, 4:37 PM
 */

#ifndef LINEWRITER_HPP
#define	LINEWRITER_HPP

#include "libhdfs/hdfs.h"

using std::string;

class LineWriter{
private:
    hdfsFS fs;
    hdfsFile file;
    string separator;
    void write(string o);
public:
    LineWriter(hdfsFS fs, hdfsFile file);
    LineWriter(hdfsFS fs, hdfsFile file, string separator);
    void write(string key, string value);
};

#endif	/* LINEWRITER_HPP */

