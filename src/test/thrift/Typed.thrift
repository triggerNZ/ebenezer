#@namespace scala au.com.cba.omnia.ebenezer.test

struct Nested {
  1: string value
}

enum SomethingOrOther {
  SOME, OTHER
}

struct Boolish {
  1: bool value
}

struct Doublish {
  1: double value
}

struct Bytish {
  1: byte value
}

struct Shortish {
  1: i16 value
}

struct Intish {
  1: i32 value
}

struct Longish {
  1: i64 value
}

struct Stringish {
  1: string value
}

struct Nestedish {
  1: Nested value
}

struct Listish {
  1: list<string> values
}

struct Mapish {
  1: map<string, string> values
}

struct Enumish {
  1: SomethingOrOther value
}
