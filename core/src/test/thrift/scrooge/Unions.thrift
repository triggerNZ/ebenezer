//   Copyright 2015 Commonwealth Bank of Australia
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

#@namespace scala au.com.cba.omnia.ebenezer.test

include "Customer.thrift"
include "CustomerNew.thrift"

union Customers {
  1: Customer.Customer       oldCustomer;
  2: CustomerNew.CustomerNew newCustomer;
}

union NestedCustomers {
  1: Customers nestedA;
  2: Customers nestedB;
}

enum AOrB { a; b; }

struct UnionStruct {
  1: required AOrB constructor;
  2: optional NestedCustomers nested;
}

union WideUnion {
  // 99 fields (of one and the same type i32)
  // you really want to have this written by a machine...
  1: i32 a1;
  2: i32 a2;
  3: i32 a3;
  4: i32 a4;
  5: i32 a5;
  6: i32 a6;
  7: i32 a7;
  8: i32 a8;
  9: i32 a9;
  10: i32 a10;
  11: i32 a11;
  12: i32 a12;
  13: i32 a13;
  14: i32 a14;
  15: i32 a15;
  16: i32 a16;
  17: i32 a17;
  18: i32 a18;
  19: i32 a19;
  20: i32 a20;
  21: i32 a21;
  22: i32 a22;
  23: i32 a23;
  24: i32 a24;
  25: i32 a25;
  26: i32 a26;
  27: i32 a27;
  28: i32 a28;
  29: i32 a29;
  30: i32 a30;
  31: i32 a31;
  32: i32 a32;
  33: i32 a33;
  34: i32 a34;
  35: i32 a35;
  36: i32 a36;
  37: i32 a37;
  38: i32 a38;
  39: i32 a39;
  40: i32 a40;
  41: i32 a41;
  42: i32 a42;
  43: i32 a43;
  44: i32 a44;
  45: i32 a45;
  46: i32 a46;
  47: i32 a47;
  48: i32 a48;
  49: i32 a49;
  50: i32 a50;
  51: i32 a51;
  52: i32 a52;
  53: i32 a53;
  54: i32 a54;
  55: i32 a55;
  56: i32 a56;
  57: i32 a57;
  58: i32 a58;
  59: i32 a59;
  60: i32 a60;
  61: i32 a61;
  62: i32 a62;
  63: i32 a63;
  64: i32 a64;
  65: i32 a65;
  66: i32 a66;
  67: i32 a67;
  68: i32 a68;
  69: i32 a69;
  70: i32 a70;
  71: i32 a71;
  72: i32 a72;
  73: i32 a73;
  74: i32 a74;
  75: i32 a75;
  76: i32 a76;
  77: i32 a77;
  78: i32 a78;
  79: i32 a79;
  80: i32 a80;
  81: i32 a81;
  82: i32 a82;
  83: i32 a83;
  84: i32 a84;
  85: i32 a85;
  86: i32 a86;
  87: i32 a87;
  88: i32 a88;
  89: i32 a89;
  90: i32 a90;
  91: i32 a91;
  92: i32 a92;
  93: i32 a93;
  94: i32 a94;
  95: i32 a95;
  96: i32 a96;
  97: i32 a97;
  98: i32 a98;
  99: i32 a99;
}
