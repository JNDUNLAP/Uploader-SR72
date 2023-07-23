package data

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

func ConnectDatabase() (*sql.DB, error) {
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error loading .env file:", err)
		return nil, err
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_NAME"),
	)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database. \n%s", err)
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return db, nil
}
