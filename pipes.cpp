#include "pipes.hpp"
#include "export.h"
#include <stdio.h>
#include <string>
#include <string.h>
#include <vector>

using std::string;
using std::vector;
#define BATCH_READ 10000

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
