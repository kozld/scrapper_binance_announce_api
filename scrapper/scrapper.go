package scrapper

import (
	"crypto/sha256"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/gocolly/colly"

	"github.com/stdi0/scrapper_binance_announce_api/config"
	"github.com/stdi0/scrapper_binance_announce_api/database"
)

const BinanceAnnouncePage = "https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query?catalogId=48&pageNo=1&pageSize=15&rnd="
const RegularExpression = `,"title":"(.*?)","body`

type Scrapper struct {
	config    *config.ScrapperConfig
	collector *colly.Collector
	db        *database.Database
}

func NewScrapper(config *config.ScrapperConfig, db *database.Database) *Scrapper {

	// Create database table
	log.Println("Creating postgres table if not exist...")
	db.Conn.Exec(database.CreateTableQuery)

	collector := colly.NewCollector()
	collector.Async = false

	return &Scrapper{config, collector, db}
}

func (s *Scrapper) Scrap() {

	re := regexp.MustCompile(RegularExpression)

	s.collector.OnResponse(func(r *colly.Response) {

		match := re.FindStringSubmatch(string(r.Body))

		if len(match) > 1 {
			announce := match[1]
			log.Printf("Announce found: %s\n", announce)
			log.Println("Saving to db...")

			// Save announce to db
			err := s.saveToDB(announce)
			if err != nil {
				log.Printf("error: %s\n\n", err.Error())
				return
			}

			// If success, print ok
			log.Printf("ok\n\n")
		}
	})

	ts := time.Now().String()
	s.collector.Visit(BinanceAnnouncePage + ts)
}

func (s *Scrapper) saveToDB(text string) error {
	var alreadyExist []byte
	hash := sha256.Sum256([]byte(text))

	// Check if hash already exist
	row := s.db.Conn.QueryRow(database.SelectQuery, string(hash[:]))
	row.Scan(&alreadyExist)
	if len(alreadyExist) != 0 {
		return fmt.Errorf("announce already exist")
	}

	// If hash not exist
	err := s.db.Conn.QueryRow(database.InsertQuery, string(hash[:]), text).Err()
	// If error, try reconnect to db...
	if err != nil {
		log.Printf("error: %s", err.Error())
		log.Println("Trying reconnect to db after 3 sec...")
		time.Sleep(3 * time.Second)

		newDb, err := s.db.ReInit()
		if err != nil {
			return err
		}

		// If reconnect success
		log.Println("Successfully reconnected")
		s.db = newDb
	}

	return nil
}
