package jsvalue

// MarshalJSON lets encoding/json embed an Obj as its JSON.stringify text.
// (encoding/json still compacts it and, with HTML escaping on, escapes
// <, >, & and U+2028/2029.)
func (o *Obj) MarshalJSON() ([]byte, error) { return Stringify(o), nil }
