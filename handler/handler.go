package handler

import (
	"dynamodb-demo-app/db"
	"dynamodb-demo-app/model"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

type Handler struct {
	d db.DB
}

func New(table string) Handler {
	return Handler{db.New(table)}
}

func (h Handler) CreateUser(rw http.ResponseWriter, req *http.Request) {
	var user model.User

	err := json.NewDecoder(req.Body).Decode(&user)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("user data", user)

	err = h.d.Save(user)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	err = json.NewEncoder(rw).Encode(user.Email)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h Handler) FetchUser(rw http.ResponseWriter, req *http.Request) {
	email := req.URL.Query().Get("email")
	city := req.URL.Query().Get("city")

	log.Println("getting user with email", email, "in city", city)

	user, err := h.d.GetOne(email, city)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			http.Error(rw, err.Error(), http.StatusNotFound)
		} else {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	err = json.NewEncoder(rw).Encode(user)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h Handler) FetchUsers(rw http.ResponseWriter, req *http.Request) {
	city := mux.Vars(req)["city"]
	log.Println("city", city)

	log.Println("getting users in city", city)

	users, err := h.d.GetMany(city)

	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	err = json.NewEncoder(rw).Encode(users)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h Handler) FetchAllUsers(rw http.ResponseWriter, req *http.Request) {

	log.Println("getting all users")

	users, err := h.d.GetAll()

	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}

	err = json.NewEncoder(rw).Encode(users)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
}
