package database

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/stdi0/scrapper_binance_announce_api/config"
)

type Database struct {
	Conf *config.DatabaseConfig
	Conn *sql.DB
}

func NewDatabase(conf *config.DatabaseConfig) (*Database, error) {

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		conf.PostgresHost, conf.PostgresPort, conf.PostgresUser, conf.PostgresPassword, conf.PostgresDbName)

	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	err = conn.Ping()
	if err != nil {
		return nil, err
	}

	log.Println("Database connection established")

	return &Database{conf, conn}, nil
}

func (db *Database) ReInit() (*Database, error) {
	return NewDatabase(db.Conf)
}
