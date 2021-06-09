package gostore

import (
	"fmt"
	"log"
	"sync"
	"time"
	"xorm.io/xorm"
	xlog "xorm.io/xorm/log"
)

const mysqlDriver = "mysql"

var (
	engine          *xorm.Engine
	engineOnce      sync.Once
	engineGroup     *xorm.EngineGroup
	engineGroupOnce sync.Once
)

type XormGroupConfig struct {
	DSN      string
	SlaveDSN string
	Debug    string
}

// NewMySQLXormGroup return singleton xorm engine group instance
func NewMySQLXormGroup(cfg *XormGroupConfig) (*xorm.EngineGroup, error) {
	var err error
	engineGroupOnce.Do(func() {
		err = newXormGroup(mysqlDriver, cfg)
	})
	return engineGroup, err
}

func newXormGroup(driver string, cfg *XormGroupConfig) error {
	var err error

	// Connect to master
	master, err := xorm.NewEngine(driver, cfg.DSN)

	if err != nil {
		log.Println("[MySQL] Connect to MySQL Master error", err)
		return err
	}

	log.Println("[MySQL] Connected to MySQL Master")

	// Connect to slave
	slave1, err := xorm.NewEngine("mysql", cfg.SlaveDSN)

	if err != nil {
		log.Println("[MySQL] Connect to MySQL Slave1 error", err)
		return err
	} else {
		log.Println("[MySQL] Connected to MySQL Slave1")
	}

	master.TZLocation, _ = time.LoadLocation("UTC")
	master.DatabaseTZ, _ = time.LoadLocation("UTC")
	slave1.TZLocation, _ = time.LoadLocation("UTC")
	slave1.DatabaseTZ, _ = time.LoadLocation("UTC")

	slaves := []*xorm.Engine{slave1}
	engineGroup, err = xorm.NewEngineGroup(master, slaves, xorm.LeastConnPolicy())

	if err != nil {
		log.Println(err)
		return err
	}

	if cfg.Debug == "debug" {
		log.Println("[MySQL] XORM debug mode enabled")
		engineGroup.ShowSQL(true)
		engineGroup.Logger().SetLevel(xlog.LOG_DEBUG)
		pingErr := engineGroup.Ping()
		if pingErr != nil {
			log.Println("[MySQL] " + pingErr.Error())
		}
	}

	return err
}

// XORM Engine ----------------------------------------------------------------------------------------------------

type XormConfig struct {
	DSN   string
	Debug bool
}

func NewMySQLXorm(cfg *XormConfig) (*xorm.Engine, error) {
	var err error
	engineOnce.Do(func() {
		engine, err = newXorm(mysqlDriver, cfg)
	})
	return engine, err
}

func newXorm(driver string, cfg *XormConfig) (*xorm.Engine, error) {
	var err error

	// Connect to master
	db, err := xorm.NewEngine(driver, cfg.DSN)

	if err != nil {
		log.Println("[MySQL] Connect to database error", err)
		return nil, err
	}

	log.Println("[MySQL] Connected to database")

	engine.TZLocation, _ = time.LoadLocation("UTC")
	engine.DatabaseTZ, _ = time.LoadLocation("UTC")

	if cfg.Debug {
		log.Println("[MySQL] XORM debug mode enabled")
		engine.ShowSQL(true)
		engine.Logger().SetLevel(xlog.LOG_DEBUG)
		pingErr := engine.Ping()
		if pingErr != nil {
			log.Println("[MySQL] " + pingErr.Error())
		}
	}

	return db, err
}

func NewXormConfig(dsn string, debug bool) *XormConfig {
	return &XormConfig{
		DSN:   dsn,
		Debug: debug,
	}
}

func BuildDsn(user, password, host, port, dbname string) string {
	dsn := "%s:%s@(%s:%s)/%s?parseTime=true&charset=utf8mb4&collation=utf8mb4_unicode_ci"
	return fmt.Sprintf(dsn, user, password, host, port, dbname)
}
