package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"pierewoj/kola/logstorage"
	"strings"
)

func handler(s logstorage.Storage, w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/")
	if r.Method == http.MethodPut {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Cannot read body %s", err)
			return
		}
		put(s, key, string(body))
	} else if r.Method == http.MethodGet {
		res, err := get(s, key)
		if err != nil {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "%s", err)
		}
		fmt.Fprint(w, res)
	} else {
		fmt.Fprintf(os.Stdout, "Incorrect method: %s", r.Method)
	}
}

func main() {
	fmt.Fprintf(os.Stdout, "Starting webserv")
	provider, err := logstorage.CreateIoPriovider("/tmp/log.txt")
	if err != nil {
		panic(err)
	}
	storage, err := logstorage.CreateStorage(provider)
	if err != nil {
		panic(err)
	}
	handlerWithStorage := func(w http.ResponseWriter, r *http.Request) {
		handler(*storage, w, r)
	}
	http.HandleFunc("/", handlerWithStorage)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
