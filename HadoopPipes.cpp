#include "HadoopPipes.h"
#include "hadoop/Pipes.hh"
#include <stdio.h>
#include "export.h"
#include "libhdfs/hdfs.h"
#include "pipes.hpp"
#include <cstring>
#include <string>
#include "LineReader.hpp"

namespace hp = HadoopPipes;
using namespace hp;
using std::string;

#define BUFF_SIZE 1000
extern "C" {

    CPointer mapContext_getInputValue(CPointer mapContext) {
        MapContext* context = (MapContext*) mapContext;
        std::string value = context->getInputValue();
        return (CPointer) value.c_str();
    }

    CPointer mapContext_getInputKey(CPointer mapContext) {
        MapContext* context = (MapContext*) mapContext;
        std::string value = context->getInputKey();
        return (CPointer) value.c_str();
    }

    void context_emit(CPointer taskContext, CPointer key, CPointer value) {
        TaskContext* context = (TaskContext*) taskContext;
        std::string k((char*) key), v((char*) value);
        context->emit(k, v);
    }

    void context_emitBatch(CPointer taskContextAddr, Objptr key, Objptr value, Int32 len) {
        hp::TaskContext* context = (hp::TaskContext*) taskContextAddr;
        char** kPtr = (char**) key;
        char** vPtr = (char**) value;
        for (int i = 0; i < len; i++) {
            std::string k((char*) kPtr[i]), v((char*) vPtr[i]);
            context->emit(k, v);
        }
    }

    CPointer reduceContext_getInputValue(CPointer reduceContext) {
        ReduceContext* context = (ReduceContext*) reduceContext;
        std::string value = context->getInputValue();
        return (CPointer) value.c_str();
    }

    CPointer reduceContext_getValueSet(CPointer reduceContext) {
        ReduceContext* context = (ReduceContext*) reduceContext;
        int size = 0;
        while (context->nextValue()) {
            std::string value = context->getInputValue();
            size++;
        }
        return NULL;
    }

    int32_t reduceContext_nextValue(CPointer reduceContext) {
        ReduceContext* context = (ReduceContext*) reduceContext;
        return context->nextValue();
    }

    CPointer reduceContext_getInputKey(CPointer reduceContext) {
        ReduceContext* context = (ReduceContext*) reduceContext;
        std::string key = context->getInputKey();
        return (CPointer) key.c_str();
    }

    CPointer readInputSplitLine(CPointer reader_) {
        MloopRecordReader* reader = (MloopRecordReader*) reader_;
        //hdfsFile file = reader->getFile();
        //hdfsFS fs = reader->getFs();
        //uint64_t off = reader->getOffset();
        //uint64_t bRead = (bytes_read > reader->getLength()) ? reader->getLength() : bytes_read;
        //printf("Offset: %ld, bytes read: %ld    \n", off, bRead);
        //        char* buffer = new char[BUFF_SIZE];
        //        hdfsSeek(fs, file, off + bRead);
        //        tSize t = 1;
        //        uint64_t len = 0;
        //        bool firstRead = true;
        //        while (t > 0) {
        //            t = hdfsRead(fs, file, buffer, BUFF_SIZE);                                                                                                                                                                                                                                                                                                          
        //            if (t == 0)
        //                break;
        //            char* ptr = strchr(buffer, '\0');
        //            if (!ptr || ptr - buffer >= BUFF_SIZE)
        //                ptr = strchr(buffer, '\n');
        //            if (!ptr || ptr - buffer >= BUFF_SIZE)
        //                ptr = strchr(buffer, '\r');
        //            if (ptr != NULL && ptr - buffer < BUFF_SIZE) {
        //                len += ptr - buffer + 1; // read also the end of line
        //                if (firstRead) { // return current result;
        //                    string val("");
        //                    val.assign(buffer, len);
        //                    reader->setBytes_read(bRead + len);
        //                    delete [] buffer;
        //                    reader_setBytesRead((Int64) bRead + len);
        //                    return (CPointer) val.c_str();
        //                }
        //                break;
        //            } else len += t;
        //            firstRead = false;
        //        }
        //        delete [] buffer;
        //        if (len == 0) {
        //            if (t == 0) return NULL;
        //            else {
        //                std::string empty("");
        //                return (CPointer) empty.c_str();
        //            }
        //        }
        //        buffer = new char[len + 1];
        //        hdfsSeek(fs, file, off + bRead);
        //        hdfsRead(fs, file, buffer, len);
        //        buffer[len] = '\0';
        //        string val("");
        //        val.assign(buffer, len + 1);
        //        reader->setBytes_read(bRead + len);
        //        delete [] buffer;
        //        reader_setBytesRead((Int64) bRead + len);
        //        return (CPointer) val.c_str();
        string val("");
        LineReader* lineReader = reader->in;
        int bytesConsumed = lineReader->readLine(val);
        reader->addStart(bytesConsumed);
        //printf("Current offset: %ldBytes consumed = %d\n", reader->getStart(), bytesConsumed);
        reader_updateOffset_bytesConsumed((Int64) reader->getStart(), (Int64) bytesConsumed);
        return (CPointer) val.c_str();
    }

    void seekHdfs(CPointer reader_, Int64 pos) {
        MloopRecordReader* reader = (MloopRecordReader*) reader_;
        hdfsFile file = reader->getFile();
        hdfsFS fs = reader->getFs();
        hdfsSeek(fs, file, pos);
        reader->setStart(pos);
    }

    void setKeyValue(CPointer reader_, CPointer key, CPointer value) {
        MloopRecordReader* reader = (MloopRecordReader*) reader_;
        std::string* k = new string((char*) key);
        std::string* v = new string((char*) value);
        reader->setKeyValue(k, v);
    }
}
