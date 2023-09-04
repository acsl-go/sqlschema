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
		Indexs: []Index{
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
