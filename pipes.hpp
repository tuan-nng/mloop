// BEGIN_COPYRIGHT
// 
// Copyright 2009-2014 CRS4.
// 
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy
// of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
// 
// END_COPYRIGHT

#ifndef HADOOP_PIPES_HPP
#define HADOOP_PIPES_HPP


#include <string>

#include <hadoop/SerialUtils.hh>
#include <hadoop/Pipes.hh>
#include <stdio.h>
#include "libhdfs/hdfs.h"

namespace hu = HadoopUtils;
namespace hp = HadoopPipes;


class MloopMapper : public hp::Mapper {
private:
    char* addr;
public:

    MloopMapper(hp::MapContext& ctx);

    void map(hp::MapContext& ctx);

    void close();

    virtual ~MloopMapper();
};

class MloopReducer : public hp::Reducer {
public:

    MloopReducer() { 
    }
    MloopReducer(hp::ReduceContext& ctx);

    virtual void reduce(hp::ReduceContext& ctx);

    virtual void close();

    virtual ~MloopReducer();
};

class MloopCombiner : public MloopReducer {
public:
    MloopCombiner(hp::MapContext& ctx);
    virtual void reduce(hp::ReduceContext& ctx);
    virtual void close();
};

//struct wrap_partitioner: hp::Partitioner, bp::wrapper<hp::Partitioner>, cxx_capsule {
//  int partition(const std::string& key, int numOfReduces) {
//    return this->get_override("partition")(key, numOfReduces);
//  }
//  virtual ~wrap_partitioner() {
//    DESTROY_PYTHON_TOO(wrap_partitioner);
//  }
//};
//
//struct wrap_record_reader: hp::RecordReader, bp::wrapper<hp::RecordReader>, cxx_capsule {
//  bool next(std::string& key, std::string& value) {
//    // t = (bool got_record, str key, str value)
//    bp::tuple t = this->get_override("next")();
//    if (!t[0]) {
//      return false;
//    }
//    bp::extract<std::string> k(t[1]);
//    if (k.check()) {
//      key = k;
//    } else {
//      throw pipes_exception("RecordReader:: overloaded method is not returning a proper key.");
//    }
//    bp::extract<std::string> v(t[2]);
//    if (v.check()) {
//      value = v;
//    } else {
//      throw pipes_exception("RecordReader:: overloaded method is not returning a proper value.");
//    }
//    return true;
//  }
//  float getProgress() {
//    return this->get_override("getProgress")();
//  }
//  void close() {
//    this->get_override("close")();
//  }
//  virtual ~wrap_record_reader() {
//    DESTROY_PYTHON_TOO(wrap_record_reader);
//  }
//};

class MloopRecordReader : public hp::RecordReader {
private:
    hdfsFS fs;
    hdfsFile file;
    uint64_t offset;
    uint64_t length;
    uint64_t bytes_read;
    std::string* key;
    std::string* value;
public:
    MloopRecordReader(hp::MapContext& ctx);
    bool next(std::string& key, std::string& value);
    float getProgress();
    hdfsFS getFs();
    hdfsFile getFile();
    uint64_t getOffset();
    uint64_t getLength();
    uint64_t getBytes_read();
    void setBytes_read(uint64_t bytes);
    void setKeyValue(std::string* key, std::string* value);
    ~MloopRecordReader();
};
//
//struct wrap_record_writer: hp::RecordWriter, bp::wrapper<hp::RecordWriter>, cxx_capsule {
//  void emit(const std::string& key, const std::string& value) {
//    this->get_override("emit")(key, value);
//  }
//  void close() {
//    this->get_override("close")();
//  }
//  virtual ~wrap_record_writer() {
//    DESTROY_PYTHON_TOO(wrap_record_writer);
//  }
//};
//
//#define CREATE_AND_RETURN_OBJECT(wobj_t, obj_t, ctx_t, method_name, ctx) \
//  bp::reference_existing_object::apply<ctx_t&>::type converter;          \
//  PyObject* po_ctx = converter(ctx);                                     \
//  bp::object o_ctx = bp::object(bp::handle<>(po_ctx));                   \
//  bp::override f = this->get_override(#method_name);                     \
//  if (f) {                                                               \
//    bp::object res = f(o_ctx);                                           \
//    std::auto_ptr<wobj_t> ap = bp::extract<std::auto_ptr<wobj_t> >(res); \
//    PyObject* obj = res.ptr();                                           \
//    bp::incref(obj);                                                     \
//    wobj_t* o = ap.get();                                                \
//    ap.release();                                                        \
//    o->entering_cxx_land();                                              \
//    return o;                                                            \
//  } else {                                                               \
//    return NULL;                                                         \
//  }
//

class MloopFactory : public hp::Factory {
private:
    int32_t combine;
    int32_t reader;
    int32_t writer;
public:

    MloopFactory(int32_t combine, int32_t reader, int32_t writer) {
        this->combine = combine;
        this->reader = reader;
        this->writer = writer;
    }

    hp::Mapper* createMapper(hp::MapContext& ctx) const {
        return new MloopMapper(ctx);
    }

    hp::Reducer* createReducer(hp::ReduceContext& ctx) const {
        return new MloopReducer(ctx);
    }

    hp::RecordReader* createRecordReader(hp::MapContext& ctx) const {
        if (reader)
            return new MloopRecordReader(ctx);
        return NULL;
    }

    hp::Reducer* createCombiner(hp::MapContext& ctx) const {
        if (combine)
            return new MloopCombiner(ctx);
        return NULL;
    }

    hp::Partitioner* createPartitioner(hp::MapContext& ctx) const {
        return NULL;
    }

    hp::RecordWriter* createRecordWriter(hp::ReduceContext& ctx) const {
        return NULL;
    }

};

#endif // HADOOP_PIPES_HPP
