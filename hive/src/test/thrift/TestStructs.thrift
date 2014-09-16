//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

#@namespace scala au.com.cba.omnia.ebenezer.scrooge.hive

struct Primitives {
  1: bool   boolean
  2: byte   byte
  3: i16    short
  4: i32    integer
  5: i64    long
  6: double double
  7: string string
}

struct Listish {
  1: i16 short
  2: list<i32> list
}

struct Mapish {
  1: i16 short
  2: map<i32, string> map
}

struct Nested {
  1: map<i32, map<string, list<i32>>> nested
}
