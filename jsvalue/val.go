package jsvalue

// Val returns the value stored under k, or nil when k is absent (like
// reading a missing JS property as undefined in a nullish check).
func (o *Obj) Val(k string) any {
	v, _ := o.Get(k)
	return v
}
