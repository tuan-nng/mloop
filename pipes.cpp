#include "pipes.hpp"
#include "export.h"
#include <stdio.h>
#include <string>
#include <string.h>
//#include <vector>

using std::string;
//using std::vector;
#define BATCH_READ 10000

MloopMapper::MloopMapper(hp::MapContext& ctx) {
    printf("init mapper\n");
    hp::MapContext* ptr = &ctx;
    init_map((CPointer) ptr);
}

void MloopMapper::map(hp::MapContext& ctx) {
    //string key = ctx.getInputKey();
    //string value = ctx.getInputValue();
    //mloop_map_((CPointer) key.c_str(), (CPointer) value.c_str());
    mloop_map_();
}

void MloopMapper::close() {
    map_flush();
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
    /*
    std::string key = ctx.getInputKey();
    vector<char*> data;
    while (ctx.nextValue()) {
        string value = ctx.getInputValue();
        char* c = new char[value.length() + 1];
        strcpy(c, value.c_str());
        data.push_back(c);
    }

    int size = data.size();
    const char** valueSet = new const char*[size];
    for (int i = 0; i < size; i++) {

        valueSet[i] = data[i];
    }
    mloop_reduce_((CPointer) key.c_str(), (CPointer) valueSet, size);
    for (int i = 0; i < size; i++) delete [] valueSet[i];
    delete []valueSet; */
    mloop_reduce_();
}

void MloopReducer::close() {
    reduce_flush();
}

MloopReducer::~MloopReducer() {
    printf("delete reducer\n");
}

// Combiner

MloopCombiner::MloopCombiner(hp::MapContext& ctx) {

    printf("init combiner\n");
}

void MloopCombiner::reduce(hp::ReduceContext& ctx) {
    //    hp::ReduceContext* ptr = &ctx;
    //    std::string key = ctx.getInputKey();
    //    vector<string> data;
    //    while (ctx.nextValue()) {
    //        string value = ctx.getInputValue();
    //        data.push_back(value);
    //    }
    //
    //    int size = data.size();
    //    const char** valueSet = new const char*[size];
    //    for (int i = 0; i < size; i++) {
    //
    //        valueSet[i] = data[i].c_str();
    //    }
    //    mloop_combine_((CPointer) ptr, (CPointer) key.c_str(), (CPointer) valueSet, size);
    //    delete []valueSet;
    //    data.clear();
}

void MloopCombiner::close() {
}
