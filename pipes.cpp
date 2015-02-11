#include "pipes.hpp"
#include "export.h"
#include <stdio.h>
#include <string>
#include <string.h>
#include <vector>
#include "libhdfs/hdfs.h"
#include "hadoop/SerialUtils.hh"
#include <sstream>
#include "pipes_serial_utils.hpp"
#include "hdfs_fs.h"
#include "utils.h"
#include <stdlib.h>

#include <iostream>
#include <sys/param.h>

using std::string;

#define BATCH_READ 10000

namespace hu = HadoopUtils;

MloopMapper::MloopMapper(hp::MapContext& ctx) {
    printf("init mapper\n");
    hp::MapContext* ptr = &ctx;
    init_map((CPointer) ptr);
}

void MloopMapper::map(hp::MapContext& ctx) {
    string key = ctx.getInputKey();
    string value = ctx.getInputValue();
    mloop_map_((CPointer) key.c_str(), (CPointer) value.c_str());
}

void MloopMapper::close() {
}

MloopMapper::~MloopMapper() {
    printf("delete mapper\n");
}


// Reducer

MloopReducer::MloopReducer(hp::ReduceContext& ctx) {
    printf("init reducer\n");
    hp::ReduceContext* ptr = &ctx;
    init_reduce((CPointer) ptr);
}

void MloopReducer::reduce(hp::ReduceContext& ctx) {
    std::string key = ctx.getInputKey();
    mloop_reduce_((CPointer) key.c_str());
}

void MloopReducer::close() {
}

MloopReducer::~MloopReducer() {
    printf("delete reducer\n");
}

// Combiner

MloopCombiner::MloopCombiner(hp::MapContext& ctx) {

    printf("init combiner\n");
}

void MloopCombiner::reduce(hp::ReduceContext& ctx) {
        hp::ReduceContext* ptr = &ctx;
        std::string key = ctx.getInputKey();     
        mloop_combine_((CPointer) ptr, (CPointer) key.c_str());
}

void MloopCombiner::close() {
}

//Record Reader

MloopRecordReader::MloopRecordReader(hp::MapContext& ctx) {
    setenv("CLASSPATH", get_hadoop_classpath("/home/hadoop-2.2.0"), 1);
    key = value = NULL;
    string split = ctx.getInputSplit();
    _StringInStream is(split);
    string filename;
    hu::deserializeString(filename, is);
    offset = is.readLong();
    length = is.readLong();
    int pos = filename.find('/', 7);
    printf("Filename: %s, Offset: %ld, length: %ld \n", filename.c_str(), offset, length);
    fs = hdfsConnect("default", 0);
    file = hdfsOpenFile(fs, filename.substr(pos).c_str(), O_RDONLY,
            0, 0, 0);
    //hdfsSeek(fs, file, offset);
    bytes_read = 0;
    reader_init((CPointer) this, (Int64) offset, (Int64) length);
    reader_setup();
    printf("end of init...\n");
}

bool MloopRecordReader::next(std::string& key_, std::string& value_) {
    int32_t res = reader_nextVal();
    if (res) {
        key_ = *key;
        value_ = *value;
        return true;
    }
    printf("+++++++++END this split**********\n");
    return false;
}

void MloopRecordReader::setKeyValue(std::string* k, std::string* val) {
    if (key) {
        delete key;
        delete value;
    }
    key = k;
    value = val;
}

float MloopRecordReader::getProgress() {
    if (length == 0)
        return 0.0;
    return MIN(1.0, (double) bytes_read / (double) length);
}

uint64_t MloopRecordReader::getBytes_read() {
    return bytes_read;
}

hdfsFile MloopRecordReader::getFile() {
    return file;
}

hdfsFS MloopRecordReader::getFs() {
    return fs;
}

uint64_t MloopRecordReader::getLength() {
    return length;
}

uint64_t MloopRecordReader::getOffset() {
    return offset;
}

void MloopRecordReader::setBytes_read(uint64_t bytes) {
    bytes_read = bytes;
}

MloopRecordReader::~MloopRecordReader() {
    hdfsCloseFile(fs, file);
}

