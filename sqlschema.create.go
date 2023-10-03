package sqlschema

import (
	"context"
	"database/sql"
)

func (sc *Schema) Create(db *sql.DB, ctx context.Context) error {
	var err error
	var sql string
	var args []interface{}

	sql = "CREATE TABLE IF NOT EXISTS `" + sc.Name + "` ("
	for _, field := range sc.Fields {
		sql += "`" + field.Name + "` " + field.Type
		if field.Nullable {
			sql += " NULL"
		} else {
			sql += " NOT NULL"
		}
		if field.AutoIncrement {
			sql += " AUTO_INCREMENT"
		}
		if field.DefaultValue != "" {
			sql += " DEFAULT " + field.DefaultValue
		}
		if field.Comment != "" {
			sql += " COMMENT '" + escape(field.Comment) + "'"
		}
		sql += ","
	}
	for _, index := range sc.Indices {
		if index.Primary {
			sql += "PRIMARY KEY ("
		} else if index.Unique {
			sql += "UNIQUE KEY `" + index.Name + "` ("
		} else {
			sql += "KEY `" + index.Name + "` ("
		}
		for _, column := range index.Columns {
			sql += "`" + column + "`,"
		}
		sql = sql[:len(sql)-1] + "),"
	}
	sql = sql[:len(sql)-1] + ")"
	if sc.Engine != "" {
		sql += " ENGINE=" + sc.Engine
	}

	if sc.Collate != "" {
		sql += " COLLATE=" + sc.Collate
	}

	if sc.Comment != "" {
		sql += " COMMENT='" + escape(sc.Comment) + "'"
	}

	_, err = db.ExecContext(ctx, sql, args...)
	if err != nil {
		return err
	}
	return nil
}
