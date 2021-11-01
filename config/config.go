package config

import (
	"github.com/caarlos0/env/v6"
	"log"
)

type ScrapperConfig struct {
}

type DatabaseConfig struct {
	PostgresHost     string `env:"POSTGRES_HOST" envDefault:"postgres"`
	PostgresPort     int    `env:"POSTGRES_PORT" envDefault:"5432"`
	PostgresUser     string `env:"POSTGRES_USER" envDefault:"postgres"`
	PostgresPassword string `env:"POSTGRES_PASSWORD,required"`
	PostgresDbName   string `env:"POSTGRES_DB" envDefault:"binance_api"`
}

func GetScrapperConfig() *ScrapperConfig {
	cfg := &ScrapperConfig{}
	if err := env.Parse(cfg); err != nil {
		log.Fatal("Cannot parse initial ENV vars: ", err)
	}

	return cfg
}

func GetDatabaseConfig() *DatabaseConfig {
	cfg := &DatabaseConfig{}
	if err := env.Parse(cfg); err != nil {
		log.Fatal("Cannot parse initial ENV vars: ", err)
	}

	return cfg
}
