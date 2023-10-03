package sqlschema

/*
The column information could be defined in the struct tag with the following format:
`db:"<column_name> <column_type> [options...]"`
The options could be a set of the following:

	pk						- Primary Key
	ai						- Auto Increment
	null					- Nullable
	unsigned				- Unsigned
	def(<value>)			- Default Value
	arr(<delimiter>) 		- Mark the column as array with the given delimiter, the default delimiter is comma(,)
	json					- Mark the column as json data
	yaml					- Mark the column as yaml data
	unique(<index_name>)	- Mark the column as a part of unique index with the given index name
	index(<index_name>)		- Mark the column as a part of index with the given index name
	comment(<comment_text>) - Append comment for the field

The column_name could be omitted, if omitted, the field name will be used as column name.
The column_type could be omitted, if omitted, the type will be determined by the field type, see below.
Only one primary key could exist in a table, if more than one column is marked as primary key, a composite primary key will be created.
The index_name could be omitted, if omitted, the the column name with a prefix('idx_') will be used as index name.
If more than one column is marked as a part of the same index, a composite index will be created.
Only one index could be defined for a column, the `unique` and `index` option could NOT be used together.
For compatibility reason, json column will be treated as text column in MySQL, and decode to json when query.

The column type could be one of the following:

	tinyint(<length>)		- Tiny Integer, the length is optional, if omitted, the default value 4 will be used
	int(<length>)			- Integer, the length is optional, if omitted, the default value 11 will be used
	bigint(<length>)		- Big Integer, the length is optional, if omitted, the default value 20 will be used
	float 					- Float
	double					- Double
	decimal(<l>, <d>)		- Decimal, the length(l) and decimals(d) are optional, if omitted, the default value 10 and 0 will be used
	varchar(<length>)		- Varchar, the length is optional, if omitted, the default value 64 will be used
	text					- Text 64k
	mediumtext				- Medium Text 16M
	longtext				- Long Text 4G
	blob					- Blob 64k
	mediumblob				- Medium Blob 16M
	longblob				- Long Blob 4G
	timestamp				- Timestamp
	datetime				- Datetime

The column type could be omitted, if omitted, the type will be determined by the field type in the struct with the following rules:

	int8, int16, int32						- int(11)
	int, int64,								- bigint(20)
	uint8, uint16, uint32					- int(11) with `unsigned` option
	uint, uint64							- bigint(20) with `unsigned` option
	float32									- float
	float64									- double
	string									- varchar(64)
	[]byte									- blob
	[]<type>								- Array of <type>, the <type> could be int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint, float32, float64 and string
											  The array will be encoded to string and stored as mediumtext in database
	other									- Serialized to json and stored as mediumtext in database
*/

import (
	"reflect"
	"strings"
	"sync"
)

const (
	// NONE for None
	NONE = 0

	// Serialize Types
	ARRAY = 1
	JSON  = 2
	YAML  = 3

	// Index Types
	INDEX       = 1
	UNIQUE      = 2
	PRIMARY_KEY = 3
)

type dataSchemaField struct {
	Name               string       // Name of the field in struct
	FieldType          reflect.Kind // Type of the field
	ColumnName         string       // Name of the column in database
	IsPrimaryKey       bool         // pk
	IsAutoincrement    bool         // ai
	IsNullable         bool         // null
	DataStoreType      string       // column_type
	DefaultValue       string       // def()
	SerializeMethod    uint8        // arr | json | yaml
	SerializeDelimiter string       // delimiter
	IndexType          uint8        // pk | index | unique
	indexName          string       // index name
	Comment            string       // comment()
}

type dataSchemaInfo struct {
	Fields []dataSchemaField
}

var dataSchemaCache = sync.Map{}

func escapeOptionParameter(p string) string {
	s := []byte(p)
	d := make([]byte, len(s))
	j := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			d[j] = s[i+1]
			i++
		} else if s[i] == ')' {
			break
		} else {
			d[j] = s[i]
		}
		j++
	}
	return string(d[:j])
}

// Parse option string like x(y), y should ending with ')', character ')' in y could be escaped with a leading slash (\).
// The return values will be: x, y
func parseOption(option string) (string, string) {
	eox := strings.Index(option, "(")
	if eox < 0 {
		return option, ""
	}
	return option[:eox], escapeOptionParameter((option[eox+1:]))
}

func parseFieldTag(field *dataSchemaField, tag string) {
	parts := strings.Split(tag, " ")
	for _, p := range parts {
		if p == "" {
			continue
		}
		if field.ColumnName == "" {
			field.ColumnName = p
			continue
		}
		option, param := parseOption(p)
		switch option {
		case "pk":
			field.IsPrimaryKey = true
			field.IndexType = PRIMARY_KEY
			field.indexName = "PRIMARY"
		case "ai":
			field.IsAutoincrement = true
		case "null":
			field.IsNullable = true
		case "unsigned":
			field.DataStoreType += " unsigned"
		case "def":
			field.DefaultValue = param
		case "arr":
			field.SerializeMethod = ARRAY
			field.SerializeDelimiter = param
		case "json":
			field.SerializeMethod = JSON
		case "yaml":
			field.SerializeMethod = YAML
		case "unique":
			field.IndexType = UNIQUE
			field.indexName = param
		case "index":
			field.IndexType = INDEX
			field.indexName = param
		case "comment":
			field.Comment = param
		case "tinyint":
			field.DataStoreType = "tinyint"
			if param != "" {
				field.DataStoreType += "(" + param + ")"
			} else {
				field.DataStoreType += "(4)"
			}
		case "int":
			field.DataStoreType = "int"
			if param != "" {
				field.DataStoreType += "(" + param + ")"
			} else {
				field.DataStoreType += "(11)"
			}
		case "bigint":
			field.DataStoreType = "bigint"
			if param != "" {
				field.DataStoreType += "(" + param + ")"
			} else {
				field.DataStoreType += "(20)"
			}
		case "float":
			field.DataStoreType = "float"
		case "double":
			field.DataStoreType = "double"
		case "decimal":
			field.DataStoreType = "decimal"
			if param != "" {
				field.DataStoreType += "(" + param + ")"
			} else {
				field.DataStoreType += "(10,0)"
			}
		case "varchar":
			field.DataStoreType = "varchar"
			if param != "" {
				field.DataStoreType += "(" + param + ")"
			} else {
				field.DataStoreType += "(64)"
			}
		case "text":
			field.DataStoreType = "text"
		case "mediumtext":
			field.DataStoreType = "mediumtext"
		case "longtext":
			field.DataStoreType = "longtext"
		case "blob":
			field.DataStoreType = "blob"
		case "mediumblob":
			field.DataStoreType = "mediumblob"
		case "longblob":
			field.DataStoreType = "longblob"
		case "timestamp":
			field.DataStoreType = "timestamp"
		case "datetime":
			field.DataStoreType = "datetime"
		}
	}
	if field.IndexType != NONE && field.indexName == "" {
		field.indexName = "idx_" + field.Name
	}
}

func loadDataSchemaInfo(v reflect.Type) *dataSchemaInfo {
	if pInfo, ok := dataSchemaCache.Load(v); ok {
		return pInfo.(*dataSchemaInfo)
	}
	info := dataSchemaInfo{}
	fieldCount := v.NumField()
	info.Fields = make([]dataSchemaField, fieldCount)
	for i := 0; i < fieldCount; i++ {
		field := v.Field(i)
		if tag, ok := field.Tag.Lookup("db"); ok {
			fieldInfo := dataSchemaField{
				Name:      field.Name,
				FieldType: field.Type.Kind(),
			}
			parseFieldTag(&fieldInfo, tag)
			info.Fields[i] = fieldInfo
		}
	}
	pInfo, _ := dataSchemaCache.LoadOrStore(v, &info)
	return pInfo.(*dataSchemaInfo)
}

func followPointer(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Ptr && !v.IsNil() {
		return followPointer(v.Elem())
	}
	return v
}

func GetSchema(v any) *Schema {
	rv := reflect.ValueOf(v)
	elem := followPointer(rv)

	if elem.Kind() != reflect.Struct || elem.IsNil() || !elem.IsValid() {
		return nil
	}

	schema := loadDataSchemaInfo(reflect.TypeOf(elem.Interface()))

	ret := &Schema{
		Fields:  make([]Field, 0, len(schema.Fields)),
		Indices: make([]Index, 0, len(schema.Fields)),
	}
	for i := 0; i < len(schema.Fields); i++ {
		field := &schema.Fields[i]

		if field.ColumnName == "" {
			field.ColumnName = field.Name
		}
	}
}
