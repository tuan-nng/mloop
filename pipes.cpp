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
#include "utils.h"
#include <stdlib.h>

#include <iostream>
#include <sys/param.h>

using std::string;

#define BATCH_READ 10000

namespace hu = HadoopUtils;
using namespace HadoopPipes;

MloopMapper::MloopMapper(hp::MapContext& ctx) {
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
}


// Reducer

MloopReducer::MloopReducer(hp::ReduceContext& ctx) {
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
}

// Combiner

MloopCombiner::MloopCombiner(hp::MapContext& ctx) {
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
    char* home = getenv("HADOOP_HOME");
    setenv("CLASSPATH", get_hadoop_classpath(home), 1);
    key = value = NULL;
    string split = ctx.getInputSplit();
    _StringInStream is(split);
    string filename;
    hu::deserializeString(filename, is);
    offset = is.readLong();
    length = is.readLong();
    int pos = filename.find('/', 7);
    
    fs = hdfsConnect("default", 0);
    file = hdfsOpenFile(fs, filename.substr(pos).c_str(), O_RDONLY,
            0, 0, 0);
    bytes_read = 0;
    start = offset;
    in = new LineReader(fs, file);
    newLine = NULL;
    reader_init((CPointer) this, (Int64) offset, (Int64) length);
        reader_setup();
}

bool MloopRecordReader::next(std::string& key_, std::string& value_) {
    int32_t res = reader_nextVal();
    if (res) {
        key_ = *key;
        value_ = *value;
        return true;
    }
    return false;
}

void MloopRecordReader::setKeyValue(std::string* k, std::string* val) {
    if (key != NULL) {
        delete key;
        delete value;
    }
    key = k;
    value = val;
}

float MloopRecordReader::getProgress() {
    if (length == 0)
        return 0.0;
    return MIN(1.0, (double) (start - offset) / (double) length);
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

uint64_t MloopRecordReader::getStart() {
    return start;
}

void MloopRecordReader::setStart(uint64_t start) {
    this->start = start;
}

void MloopRecordReader::addStart(int bytesConsumed) {
    this->start += bytesConsumed;
}

char*& MloopRecordReader::getNewLine() {
    return newLine;
}

void MloopRecordReader::close() {
    in->close();
    hdfsCloseFile(fs, file);
    if (newLine != NULL)
        delete []newLine;
}

MloopRecordReader::~MloopRecordReader() {
    if (key != NULL) {
        delete key;
        delete value;
    }
    delete in;
}

MloopRecordWriter::MloopRecordWriter(hp::ReduceContext& ctx) {
    char* home = getenv("HADOOP_HOME");
    setenv("CLASSPATH", get_hadoop_classpath(home), 1);
    const JobConf* conf = ctx.getJobConf();
    string out_dir = conf->get("mapreduce.output.fileoutputformat.outputdir");
    int pos = out_dir.find('/', 7);
    out_dir = out_dir.substr(pos);
    int part = conf->getInt("mapred.task.partition");
    char s[200];
    sprintf(s, "%s/part-%.5d", out_dir.c_str(), part);
    fs = hdfsConnect("default", 0);
    file = hdfsOpenFile(fs, s, O_WRONLY | O_CREAT,
            0, 0, 0);
    string sep("\t");
    if (conf->hasKey("mapred.textoutputformat.separator"))
        sep = conf->get("mapred.textoutputformat.separator");
    writer = new LineWriter(fs, file, sep);
    writer_init((CPointer) this);
}

LineWriter* MloopRecordWriter::getWriter() {
    return writer;
}

void MloopRecordWriter::emit(const std::string& key, const std::string& value) {
    mloop_write((CPointer) key.c_str(), (CPointer) value.c_str());
}

void MloopRecordWriter::close() {
    hdfsCloseFile(fs, file);
}

MloopRecordWriter::~MloopRecordWriter() {
    delete writer;
}


