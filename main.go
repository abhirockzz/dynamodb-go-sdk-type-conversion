package main

import (
	"dynamodb-demo-app/handler"
	"dynamodb-demo-app/util"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/gorilla/mux"
)

const defaultTable = "users"
const defaultPort = "8080"

var h handler.Handler
var port string

func init() {
	table := os.Getenv("TABLE_NAME")
	if table == "" {
		table = defaultTable
		log.Println("missing environment variable TABLE_NAME. using default value -", defaultTable)
	}

	port = os.Getenv("PORT")
	if port == "" {
		port = defaultPort
		log.Println("missing environment variable PORT. using default value -", defaultPort)
	}

	seedTestDataStr := os.Getenv("SEED_TEST_DATA")
	seedTestData, _ := strconv.ParseBool(seedTestDataStr)

	if seedTestData {
		util.Seed(table)
	}

	h = handler.New(table)
}

func main() {
	router := mux.NewRouter()
	router.HandleFunc("/user", h.CreateUser).Methods(http.MethodPost)
	router.HandleFunc("/user", h.FetchUser).Methods(http.MethodGet)
	router.HandleFunc("/users/{city}", h.FetchUsers).Methods(http.MethodGet)
	router.HandleFunc("/users/", h.FetchAllUsers).Methods(http.MethodGet)

	log.Println("started http server...")
	log.Fatal(http.ListenAndServe(":"+port, router))
}
