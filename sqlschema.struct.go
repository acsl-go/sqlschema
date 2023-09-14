package sqlschema

type Field struct {
	Name          string
	Type          string
	Nullable      bool
	AutoIncrement bool
	DefaultValue  string
	Comment       string
}

type Index struct {
	Name    string
	Columns []string
	Primary bool
	Unique  bool
}

type Schema struct {
	Name    string
	Fields  []Field
	Indices []Index
	Engine  string
	Collate string
	Comment string
}

func (sc *Schema) Field(name string) *Field {
	for _, field := range sc.Fields {
		if field.Name == name {
			return &field
		}
	}
	return nil
}

func (sc *Schema) Index(name string) *Index {
	if name == "PRIMARY" {
		name = ""
	}
	for _, index := range sc.Indices {
		if index.Name == name || (name == "" && index.Primary) {
			return &index
		}
	}
	return nil
}

func (fd *Field) Equal(other *Field) bool {
	if fd.Name != other.Name {
		return false
	}
	if fd.Type != other.Type {
		return false
	}
	if fd.Nullable != other.Nullable {
		return false
	}
	if fd.AutoIncrement != other.AutoIncrement {
		return false
	}
	defVal1 := fd.DefaultValue
	defVal2 := other.DefaultValue
	if defVal1 == "NULL" {
		defVal1 = ""
	}
	if defVal2 == "NULL" {
		defVal2 = ""
	}
	if defVal1 != defVal2 {
		return false
	}
	if fd.Comment != other.Comment {
		return false
	}
	return true
}

func (idx *Index) Equal(other *Index) bool {
	if idx.Primary != other.Primary {
		return false
	}
	if !idx.Primary && idx.Name != other.Name {
		return false
	}
	if idx.Unique != other.Unique {
		return false
	}
	if len(idx.Columns) != len(other.Columns) {
		return false
	}
	for i, column := range idx.Columns {
		if column != other.Columns[i] {
			return false
		}
	}
	return true
}
