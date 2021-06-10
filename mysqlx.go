package gostore

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"log"
	"sync"
)

type MySqlxConfig struct {
	Dsn string
}

var (
	mysqlDbx     *sqlx.DB
	mysqlDbxOnce sync.Once
)

func NewSqlx(cfg MySqlxConfig) (*sqlx.DB, error) {
	var err error
	mysqlDbxOnce.Do(func() {
		mysqlDbx, err = newSqlx(cfg)
	})
	return mysqlDbx, err
}

func newSqlx(cfg MySqlxConfig) (*sqlx.DB, error) {
	db, err := sqlx.Connect(mysqlDriver, cfg.Dsn)

	if err != nil {
		log.Println("[MySQL] Connect to database error")
		return nil, err
	}

	return db, nil
}
