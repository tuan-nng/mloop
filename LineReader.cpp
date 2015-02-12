#include "LineReader.hpp"
#include "export.h"
#include <sys/param.h>
#include <stdio.h>

LineReader::LineReader(hdfsFS fs_, hdfsFile file_) {
    this->fs = fs_;
    this->file = file_;
    this->bufferSize = DEFAULT_BUFF_SIZE;
    this->buffer = new char[bufferSize];
    bufferLen = bufferPosn = 0;
}

bool LineReader::backfill() {
    bufferPosn = 0;
    bufferLen = hdfsRead(fs, file, buffer, bufferSize);
    return (bufferLen > 0);
}

int LineReader::readLine(string& str, int maxLineLength, int maxBytesToConsume) {
    str.clear();
    bool hadFinalNewline = false;
    bool hadFinalReturn = false;
    bool hitEndofFile = false;
    int startPosn = bufferPosn;
    long bytesConsumed = 0;
    while (true) {
        if (bufferPosn >= bufferLen) {
            if (!backfill()) {
                hitEndofFile = true;
                break;
            }
        }
        startPosn = bufferPosn;

        for (; bufferPosn < bufferLen; bufferPosn++) {
            switch (buffer[bufferPosn]) {
                case '\n':
                    hadFinalNewline = true;
                    bufferPosn += 1;
                    goto end_while;
                case '\r':
                    if (hadFinalReturn)
                        goto end_while;
                    hadFinalReturn = true;
                    break;
                default:
                    if (hadFinalReturn)
                        goto end_while;
            }
        }

        // only go here when \r at the end of buffer or not find \n,\r
        bytesConsumed += bufferPosn - startPosn;
        int length = bufferPosn - startPosn - (hadFinalReturn ? 1 : 0);
        length = MIN(length, maxLineLength - str.length());
        if (length >= 0)
            str.append(buffer, startPosn, length);
        if (bytesConsumed >= maxBytesToConsume)
            return (int) MIN(bytesConsumed, (long) INT32_MAX);
    }
end_while:
    int newLineLength = (hadFinalNewline ? 1 : 0) + (hadFinalReturn ? 1 : 0);
    if (!hitEndofFile) {
        bytesConsumed += bufferPosn - startPosn;
        int length = bufferPosn - startPosn - newLineLength;
        length = (int) MIN(length, maxLineLength - str.length());
        if (length > 0)
            str.append(buffer, startPosn, length);
    }
    return (int) MIN(bytesConsumed, (long) INT32_MAX);
}

int LineReader::readLine(string& str, int maxLineLength) {
    return readLine(str, maxLineLength, INT32_MAX);
}

int LineReader::readLine(string& str) {
    return readLine(str, INT32_MAX, INT32_MAX);
}

void LineReader::close() {
    printf("delete buffer\n");
    delete buffer;
}

LineReader::~LineReader() {
    printf("fucking shit.\n");
}
