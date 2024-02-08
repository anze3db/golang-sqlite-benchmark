package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

var DB *sql.DB

func fail(w http.ResponseWriter, err error) {
	log.Println(err)
	w.WriteHeader(500)
	fmt.Fprint(w, "error\n")
}

func read(w http.ResponseWriter, req *http.Request) {
	rows, err := DB.Query("select name from row limit 10")
	if err != nil {
		fail(w, err)
		return
	}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		fmt.Fprintf(w, "name: %s\n", name)
	}
}

func read_transaction(w http.ResponseWriter, req *http.Request) {
	tx, err := DB.Begin()
	if err != nil {
		fail(w, err)
		return
	}
	defer tx.Rollback()
	rows, err := tx.Query("select name from row limit 10")
	if err != nil {
		fail(w, err)
		return
	}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		fmt.Fprintf(w, "name: %s\n", name)
	}
	tx.Commit()
}

func write(w http.ResponseWriter, req *http.Request) {
	stmt, err := DB.Prepare("insert into row(name) values(?)")
	if err != nil {
		fail(w, err)
		return
	}
	defer stmt.Close()

	_, err = stmt.Exec("name")
	if err != nil {
		fail(w, err)
		return
	}
	fmt.Fprint(w, "ok\n")
}

func read_write(w http.ResponseWriter, req *http.Request) {
	rows, err := DB.Query("select name from row limit 10")
	if err != nil {
		fail(w, err)
		return
	}

	stmt, err := DB.Prepare("insert into row(name) values(?)")
	if err != nil {
		fail(w, err)
		return
	}
	defer stmt.Close()

	_, err = stmt.Exec("name")
	if err != nil {
		fail(w, err)
		return
	}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		fmt.Fprintf(w, "name: %s\n", name)
	}
}

func write_read(w http.ResponseWriter, req *http.Request) {
	stmt, err := DB.Prepare("insert into row(name) values(?)")
	if err != nil {
		fail(w, err)
		return
	}
	defer stmt.Close()

	_, err = stmt.Exec("name")
	if err != nil {
		fail(w, err)
		return
	}

	rows, err := DB.Query("select name from row limit 10")
	if err != nil {
		fail(w, err)
		return
	}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		fmt.Fprintf(w, "name: %s\n", name)
	}
}

func read_write_transaction(w http.ResponseWriter, req *http.Request) {
	tx, err := DB.Begin()
	if err != nil {
		fail(w, err)
		return
	}
	defer tx.Rollback()
	rows, err := tx.Query("select name from row limit 10")
	if err != nil {
		fail(w, err)
		return
	}

	stmt, err := tx.Prepare("insert into row(name) values(?)")
	if err != nil {
		fail(w, err)
		return
	}
	defer stmt.Close()

	_, err = stmt.Exec("name")
	if err != nil {
		fail(w, err)
		return
	}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		fmt.Fprintf(w, "name: %s\n", name)
	}
	tx.Commit()
}

func write_read_transaction(w http.ResponseWriter, req *http.Request) {
	tx, err := DB.Begin()
	if err != nil {
		fail(w, err)
		return
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("insert into row(name) values(?)")
	if err != nil {
		fail(w, err)
		return
	}
	defer stmt.Close()

	_, err = stmt.Exec("name")
	if err != nil {
		fail(w, err)
		return
	}

	rows, err := tx.Query("select name from row limit 10")
	if err != nil {
		fail(w, err)
		return
	}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		fmt.Fprintf(w, "name: %s\n", name)
	}
	tx.Commit()
}

func read_write_transaction_immediate(w http.ResponseWriter, req *http.Request) {
	fmt.Fprint(w, "ok\n")
}

func main() {

	os.Remove("./db.sqlite3")

	db, err := sql.Open("sqlite3", "./db.sqlite3?_txlock=deferred")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	DB = db

	sqlStmt := `create table row(id integer not null primary key, name text);`
	if _, err = db.Exec(sqlStmt); err != nil {
		log.Fatal(err)
	}
	sqlStmt = `PRAGMA journal_mode=WAL;`
	if _, err = db.Exec(sqlStmt); err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/read/", read)
	http.HandleFunc("/read_transaction/", read_transaction)
	http.HandleFunc("/write/", write)
	http.HandleFunc("/write_read/", write_read)
	http.HandleFunc("/read_write/", read_write)
	http.HandleFunc("/write_read_transaction/", write_read_transaction)
	http.HandleFunc("/read_write_transaction/", read_write_transaction)
	http.HandleFunc("/read_write_transaction_immediate/", read_write_transaction_immediate)

	http.ListenAndServe(":8000", nil)
}
