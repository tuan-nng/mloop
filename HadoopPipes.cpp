#include "HadoopPipes.h"
#include <hadoop/Pipes.hh>
#include <stdio.h>
#include "export.h"

namespace hp = HadoopPipes;
using namespace hp;
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

    //    void mapContext_emit(CPointer mapContextAddr, char* key, char* value) {
    //        hp::MapContext* context = convertAddressStringToPointer<hp::MapContext>((char*) mapContextAddr);
    //        std::string k(key), v(value);
    //        context->emit(k, v);
    //    }

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
}
