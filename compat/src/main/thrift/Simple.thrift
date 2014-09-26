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

#@namespace scala au.com.cba.omnia.ebenezer.compat

struct Simple {
  1: string  stringfield
  2: bool    booleanfield
  3: i16     shortfield
  4: i32     intfield
  5: i64     longfield
  6: double  doublefield
  7: byte    bytefield
  8: optional string optstringfield
  9: optional double optdoublefield
}
