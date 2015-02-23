#include <string>
#include "hadoop/SerialUtils.hh"
#include "LineWriter.hpp"
#include <stdio.h>

LineWriter::LineWriter(hdfsFS fs_, hdfsFile file_, string separator_) {
    this->fs = fs_;
    this->file = file_;
    this->separator = separator_;
}

LineWriter::LineWriter(hdfsFS fs_, hdfsFile file_) {
    this->fs = fs_;
    this->file = file_;
    this->separator = "\t";
}

void LineWriter::write(string o) {
    int size = o.length();
    hdfsWrite(fs, file, (void*) o.c_str(), size);
    if (hdfsFlush(fs, file)) {
        fprintf(stderr, "Failed to flush %s\n", o.c_str());
        throw HadoopUtils::Error("Failed to flush data.\n", __FILE__, __LINE__, __func__);
    }
}

void LineWriter::write(string key, string value) {
    string val(key);
    if (key.length() > 0 || value.length() > 0)
        val += separator;
    val += value;
    val += "\n";
    write(val);
}



