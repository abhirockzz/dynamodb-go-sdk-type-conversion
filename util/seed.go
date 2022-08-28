package util

import (
	"dynamodb-demo-app/db"
	"log"
	"os"
	"strconv"
)

const defaultNumItems = "100"

func Seed(table string) {
	numOfItems := os.Getenv("NUM_ITEMS")
	if numOfItems == "" {
		numOfItems = defaultNumItems
		log.Println("missing environment variable NUM_ITEMS. using default value -", defaultNumItems)
	}

	total, _ := strconv.Atoi(numOfItems)
	db.New(table).BatchImport(total)
}
