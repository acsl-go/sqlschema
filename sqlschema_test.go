package sqlschema

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func connectDB() *sql.DB {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=true&loc=Local", "root", "123456", "localhost", "test")
	db, _ := sql.Open("mysql", dsn)
	if e := db.Ping(); e != nil {
		panic(e)
	}
	return db
}

func TestSchemaReflect(t *testing.T) {
	data := &struct {
		ID      int                    `db:"id pk ai int(11)"`
		Name    string                 `db:"name unique varchar(255)"`
		Age     int                    `db:"age def(0) int(11)"`
		Idx1    int                    `db:"idx1 unique(idx)"`
		Idx2    int                    `db:"idx2 unique(idx)"`
		Comment string                 `db:"comment null"`
		Arr     []string               `db:"arr text arr(,)"`
		Json    map[string]interface{} `db:"json text json"`
		Yaml    map[string]interface{} `db:"yaml text yaml"`
	}{}
	sc := GetSchema(data)
	sc.Name = "test2"
	sc.Engine = "InnoDB"
	sc.Collate = "utf8mb4_general_ci"
	t.Log(sc)
	db := connectDB()
	defer db.Close()
	if e := sc.Update(db, context.Background()); e != nil {
		t.Error(e)
	}

	data.Name = "foo"
	data.Age = 10
	data.Idx1 = 1
	data.Idx2 = 2
	data.Comment = ""
	data.Arr = []string{"a", "b", "c"}
	data.Json = map[string]interface{}{
		"foo": "bar",
		"bar": 123,
	}
	data.Yaml = map[string]interface{}{
		"foo": "bar",
		"bar": 123,
	}
	if e := Insert(context.Background(), db, "test2", data); e != nil {
		t.Error(e)
	}

	data.Age = 20
	if e := Update(context.Background(), db, "test2", nil, data); e != nil {
		t.Error(e)
	}
}

func TestSchemaReflectScan(t *testing.T) {
	data := &struct {
		ID      int                    `db:"id pk ai int(11)"`
		Name    string                 `db:"name unique varchar(255)"`
		Age     int                    `db:"age def(0) int(11)"`
		Idx1    int                    `db:"idx1 unique(idx)"`
		Idx2    int                    `db:"idx2 unique(idx)"`
		Comment string                 `db:"comment null"`
		Arr     []string               `db:"arr text arr(,)"`
		Json    map[string]interface{} `db:"json text json"`
		Yaml    map[string]interface{} `db:"yaml text yaml"`
	}{}
	db := connectDB()
	defer db.Close()

	r, e := db.Query("select * from test2")
	if e != nil {
		t.Error(e)
	}

	for r.Next() {
		if e := ScanRrow(r, data); e != nil {
			t.Error(e)
		}
		t.Log(data)
	}
}

func TestSchemeUpdate(t *testing.T) {
	sc := &Schema{
		Name: "test",
		Fields: []Field{
			{
				Name:          "id",
				Type:          "int(11)",
				AutoIncrement: true,
			},
			{
				Name:     "name",
				Type:     "varchar(255)",
				Nullable: true,
			},
			{
				Name:     "titleX",
				Type:     "varchar(255)",
				Nullable: true,
			},
			{
				Name:         "age",
				Type:         "int(11)",
				Nullable:     false,
				DefaultValue: "0",
			},
			{
				Name:         "gender",
				Type:         "tinyint(1)",
				Nullable:     false,
				DefaultValue: "0",
				Comment:      "0 for Unknown, \"1\" for Male, '2' for Female, '3' for Other",
			},
		},
		Indices: []Index{
			{
				Columns: []string{"id"},
				Primary: true,
			},
			{
				Name:    "name",
				Columns: []string{"name"},
				Unique:  true,
			},
			{
				Name:    "title_name",
				Columns: []string{"titleX"},
			},
		},
		Engine:  "InnoDB",
		Collate: "utf8mb4_general_ci",
		Comment: "test2",
	}

	db := connectDB()
	defer db.Close()
	if e := sc.Update(db, context.Background()); e != nil {
		t.Error(e)
	}
}

func TestSchemeRead(t *testing.T) {
	db := connectDB()
	defer db.Close()
	sc, e := ReadFromDB(db, context.Background(), "test")
	if e != nil {
		t.Error(e)
	}
	t.Log(sc)
}
