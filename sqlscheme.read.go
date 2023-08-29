package sqlscheme

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"
)

func ReadFromDB(db *sql.DB, ctx context.Context, name string) (*Schema, error) {
	var dbName string
	if e := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&dbName); e != nil {
		return nil, errors.Wrap(e, "Get database name failed")
	}

	sc := &Schema{Name: name, Fields: make([]Field, 0), Indexs: make([]Index, 0)}
	if e := db.QueryRowContext(ctx, "SELECT `ENGINE`,`TABLE_COLLATION`,`TABLE_COMMENT` FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?", dbName, name).Scan(&sc.Engine, &sc.Collate, &sc.Comment); e != nil {
		if e == sql.ErrNoRows {
			return nil, nil
		}
		return nil, errors.Wrap(e, "Get table info failed")
	}

	rows, e := db.QueryContext(ctx, "SELECT `COLUMN_NAME`,`COLUMN_TYPE`,`IS_NULLABLE`,`COLUMN_DEFAULT`,`COLUMN_COMMENT`,`EXTRA` FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?", dbName, name)
	if e != nil {
		return nil, errors.Wrap(e, "Get table columns failed")
	}

	for rows.Next() {
		var field Field
		var extra, isNullable string
		var defaultValue sql.NullString
		if e := rows.Scan(&field.Name, &field.Type, &isNullable, &defaultValue, &field.Comment, &extra); e != nil {
			return nil, errors.Wrap(e, "Scan table columns failed")
		}
		if extra == "auto_increment" {
			field.AutoIncrement = true
		}
		if isNullable == "YES" {
			field.Nullable = true
		}
		if defaultValue.Valid {
			field.DefaultValue = defaultValue.String
		}
		sc.Fields = append(sc.Fields, field)
	}

	rows, e = db.QueryContext(ctx, "SELECT `INDEX_NAME`,`SEQ_IN_INDEX`,`COLUMN_NAME`,`NON_UNIQUE` FROM `information_schema`.`STATISTICS` WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?", dbName, name)
	if e != nil {
		return nil, errors.Wrap(e, "Get table indexs failed")
	}

	idxMap := make(map[string]int)
	for rows.Next() {
		var idxName string
		var idxColumn string
		var seq, nonUnique int

		if e := rows.Scan(&idxName, &seq, &idxColumn, &nonUnique); e != nil {
			return nil, errors.Wrap(e, "Scan table indexs failed")
		}

		if i, ok := idxMap[idxName]; !ok {
			idxMap[idxName] = len(sc.Indexs)
			index := Index{Name: idxName, Columns: []string{idxColumn}}
			if index.Name == "PRIMARY" {
				index.Primary = true
			} else if nonUnique == 0 {
				index.Unique = true
			}
			sc.Indexs = append(sc.Indexs, index)
		} else {
			sc.Indexs[i].Columns = append(sc.Indexs[i].Columns, idxColumn)
		}
	}

	return sc, nil
}
