package sqlscheme

import (
	"context"
	"database/sql"
)

func (sc *Schema) Update(db *sql.DB, ctx context.Context) error {
	cur, e := ReadFromDB(db, ctx, sc.Name)
	if e != nil {
		return e
	}

	if cur == nil {
		return sc.Create(db, ctx)
	}

	sql := ""
	args := make([]interface{}, 0, 10)

	if sc.Engine != cur.Engine {
		sql += " ENGINE = " + sc.Engine
	}

	if sc.Collate != cur.Collate {
		sql += " COLLATE = " + sc.Collate
	}

	if sc.Comment != cur.Comment {
		sql += " COMMENT = '" + escape(sc.Comment) + "'"
	}

	if sql != "" {
		sql = "ALTER TABLE `" + sc.Name + "`" + sql
		_, e = db.ExecContext(ctx, sql, args...)
		if e != nil {
			return e
		}
	}

	for _, field := range cur.Fields {
		if sc.Field(field.Name) == nil {
			sql = "ALTER TABLE `" + sc.Name + "` DROP `" + field.Name + "`"
			_, e = db.ExecContext(ctx, sql, args...)
			if e != nil {
				return e
			}
		}
	}

	for _, field := range sc.Fields {
		fd := cur.Field(field.Name)
		sql = ""
		if fd == nil {
			sql = "ALTER TABLE `" + sc.Name + "` ADD `" + field.Name + "` " + field.Type
		} else if !fd.Equal(&field) {
			sql = "ALTER TABLE `" + sc.Name + "` MODIFY `" + field.Name + "` " + field.Type
		}
		if sql != "" {
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
			_, e = db.ExecContext(ctx, sql, args...)
			if e != nil {
				return e
			}
		}
	}

	for _, index := range cur.Indexs {
		if sc.Index(index.Name) == nil {
			sql = "ALTER TABLE `" + sc.Name + "` DROP INDEX `" + index.Name + "`"
			_, e = db.ExecContext(ctx, sql, args...)
			if e != nil {
				return e
			}
		}
	}

	for _, index := range sc.Indexs {
		idx := cur.Index(index.Name)
		sql = ""
		if idx == nil {
			if index.Primary {
				sql = "ALTER TABLE `" + sc.Name + "` ADD PRIMARY KEY ("
			} else if index.Unique {
				sql = "ALTER TABLE `" + sc.Name + "` ADD UNIQUE KEY `" + index.Name + "` ("
			} else {
				sql = "ALTER TABLE `" + sc.Name + "` ADD KEY `" + index.Name + "` ("
			}
		} else if !idx.Equal(&index) {
			if index.Primary {
				sql = "ALTER TABLE `" + sc.Name + "` DROP PRIMARY KEY, ADD PRIMARY KEY ("
			} else if index.Unique {
				sql = "ALTER TABLE `" + sc.Name + "` DROP INDEX `" + index.Name + "`, ADD UNIQUE KEY `" + index.Name + "` ("
			} else {
				sql = "ALTER TABLE `" + sc.Name + "` DROP INDEX `" + index.Name + "`, ADD KEY `" + index.Name + "` ("
			}
		}
		if sql != "" {
			for _, column := range index.Columns {
				sql += "`" + column + "`,"
			}
			sql = sql[:len(sql)-1] + ")"
			_, e = db.ExecContext(ctx, sql, args...)
			if e != nil {
				return e
			}
		}
	}

	return nil
}
