package main

import (
	"log"
	"time"

	_ "github.com/lib/pq"

	"github.com/stdi0/scrapper_binance_announce_api/config"
	"github.com/stdi0/scrapper_binance_announce_api/database"
	"github.com/stdi0/scrapper_binance_announce_api/scrapper"
)

func main() {
	log.Println("Getting scrapper config...")
	scrapConf := config.GetScrapperConfig()

	log.Println("Getting database config...")
	dbConf := config.GetDatabaseConfig()

	var db *database.Database

	for {
		var err error
		log.Println("Trying connect to database...")
		db, err = database.NewDatabase(dbConf)
		if err != nil {
			log.Printf("error: %s", err.Error())
			log.Println("Trying reconnect after 3 sec...")
			time.Sleep(3 * time.Second)
		} else {
			break
		}
	}

	// Main loop
	for {
		log.Println("Scrapping binance page...")
		colly := scrapper.NewScrapper(scrapConf, db)
		colly.Scrap()
		log.Println("Sleep 5 sec...")
		time.Sleep(5 * time.Second)
	}
}
