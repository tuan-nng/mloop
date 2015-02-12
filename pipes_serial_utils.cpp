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

#include "hadoop/StringUtils.hh"
#include "pipes_serial_utils.hpp"

namespace hu = HadoopUtils;


_StringOutStream::_StringOutStream(): _os() {}

void _StringOutStream::write(const void *buff, std::size_t len) {
  _os.write(static_cast<const char*>(buff), len);
}

void _StringOutStream::flush() {}

std::string _StringOutStream::str() {
  return _os.str();
}


_StringInStream::_StringInStream(const std::string& s):  _is(s) {}

void _StringInStream::read(void *buff, std::size_t len) {
  _is.read(static_cast<char*>(buff), len);
}

uint16_t _StringInStream::readUShort() {
  char buf[2];
  _is.read(buf, 2);
  uint16_t t = (buf[0] << 8) + buf[1];
  return t;
}

uint64_t _StringInStream::readLong() {
  char buf[8];
  _is.read(buf, 8);
  int64_t t = 0;
  for (int i = 0; i < 8; i++) {
    t = t << 8;
    t |= (buf[i] & 0xFF);
  }
  return t;
}

void _StringInStream::seekg(std::size_t offset) {
  _is.seekg(offset);
}

std::size_t _StringInStream::tellg() {
  return _is.tellg();
}


#define PIPES_SERIALIZE_DEF(type, name, hu_name) \
std::string name(type t) {                       \
  _StringOutStream os;                           \
  hu_name(t, os);                                \
  return os.str();                               \
}

PIPES_SERIALIZE_DEF(long, pipes_serialize_int, hu::serializeLong);
PIPES_SERIALIZE_DEF(float, pipes_serialize_float, hu::serializeFloat);
PIPES_SERIALIZE_DEF(std::string, pipes_serialize_string, hu::serializeString);
