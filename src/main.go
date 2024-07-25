package main

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/slack-go/slack"
)

type RestoreRequest struct {
	RequestID      string   `json:"request_id"`
	BucketPaths    []string `json:"bucket_paths"`
	TTL            int      `json:"ttl"`
	ProcessedPaths []string `json:"processed_paths"`
	CreatedAt      string   `json:"created_at"`
	UpdatedAt      string   `json:"updated_at"`
}

func generateRequestID() string {
	bytes := make([]byte, 16)
	_, err := rand.Read(bytes)
	if err != nil {
		log.Fatalf("Failed to generate request ID: %v", err)
	}
	return hex.EncodeToString(bytes)
}

func sendSlackNotification(channel, message string) {
	slackToken := os.Getenv("SLACK_API_TOKEN")
	if slackToken == "" {
		log.Println("No SLACK_API_TOKEN set. Slack messages will not be sent.")
		return
	}

	api := slack.New(slackToken)

	_, _, err := api.PostMessage(channel, slack.MsgOptionText(message, false))
	if err != nil {
		log.Printf("Failed to send Slack message: %v\n", err)
	}
}

func createDBAndRecord(requestID string, bucketPaths []string, ttl int) error {
	db, err := sql.Open("sqlite3", "./s3_restore_requests.db")
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS restore_requests (
		request_id TEXT PRIMARY KEY,
		bucket_paths TEXT,
		ttl INTEGER,
		processed_paths TEXT,
		created_at TEXT,
		updated_at TEXT
	)`
	_, err = db.Exec(createTableQuery)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	bucketPathsJSON, _ := json.Marshal(bucketPaths)
	processedPathsJSON, _ := json.Marshal([]string{})
	insertQuery := `
	INSERT INTO restore_requests (request_id, bucket_paths, ttl, processed_paths, created_at, updated_at)
	VALUES (?, ?, ?, ?, ?, ?)`
	_, err = db.Exec(insertQuery, requestID, bucketPathsJSON, ttl, processedPathsJSON, time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("failed to insert record: %w", err)
	}

	message := fmt.Sprintf("Created database record for Request ID: %s\nBucket Paths: %s\nTTL: %d\nProcessed Paths: %s\nCreated At: %s\nUpdated At: %s\n",
		requestID, bucketPathsJSON, ttl, processedPathsJSON, time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339))
	sendSlackNotification(os.Getenv("SLACK_CHANNEL_ID"), message)

	log.Println(message)
	return nil
}

func updateProcessedPaths(requestID, processedPath string) error {
	db, err := sql.Open("sqlite3", "./s3_restore_requests.db")
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	var bucketPaths, processedPaths string

	selectQuery := "SELECT bucket_paths, processed_paths FROM restore_requests WHERE request_id = ?"
	err = db.QueryRow(selectQuery, requestID).Scan(&bucketPaths, &processedPaths)
	if err != nil {
		return fmt.Errorf("failed to select paths: %w", err)
	}

	var bp, pp []string
	json.Unmarshal([]byte(bucketPaths), &bp)
	json.Unmarshal([]byte(processedPaths), &pp)

	pp = append(pp, processedPath)
	for i, path := range bp {
		if path == processedPath {
			bp = append(bp[:i], bp[i+1:]...)
			break
		}
	}

	bucketPathsJSON, _ := json.Marshal(bp)
	processedPathsJSON, _ := json.Marshal(pp)
	updateQuery := `
	UPDATE restore_requests
	SET bucket_paths = ?, processed_paths = ?, updated_at = ?
	WHERE request_id = ?`
	_, err = db.Exec(updateQuery, bucketPathsJSON, processedPathsJSON, time.Now().UTC().Format(time.RFC3339), requestID)
	if err != nil {
		return fmt.Errorf("failed to update paths: %w", err)
	}

	message := fmt.Sprintf("Updated database record for Request ID: %s\nRemaining Bucket Paths: %s\nProcessed Paths: %s\n", requestID, bucketPathsJSON, processedPathsJSON)
	sendSlackNotification(os.Getenv("SLACK_CHANNEL_ID"), message)

	log.Println(message)

	if len(bp) == 0 {
		deleteQuery := "DELETE FROM restore_requests WHERE request_id = ?"
		_, err = db.Exec(deleteQuery, requestID)
		if err != nil {
			return fmt.Errorf("failed to delete record: %w", err)
		}
		message = fmt.Sprintf("All paths processed for Request ID: %s. Record deleted.\n", requestID)
		sendSlackNotification(os.Getenv("SLACK_CHANNEL_ID"), message)
		fmt.Print(message)
	}

	return nil
}

func restoreObject(svc *s3.S3, bucketName, key string) {
	copyInput := &s3.CopyObjectInput{
		Bucket:       aws.String(bucketName),
		CopySource:   aws.String(fmt.Sprintf("%s/%s", bucketName, key)),
		Key:          aws.String(key),
		StorageClass: aws.String("STANDARD"),
	}

	_, err := svc.CopyObject(copyInput)
	if err != nil {
		log.Printf("Failed to restore object %s: %v\n", key, err)
		return
	}

	fmt.Printf("Object %s restored to STANDARD storage class\n", key)
}

func restoreObjectsInPath(bucketPath, region, requestID string) {
	parts := strings.SplitN(bucketPath, "/", 2)
	if len(parts) < 2 {
		log.Printf("Invalid bucket path: %s\n", bucketPath)
		return
	}
	bucketName, prefix := parts[0], parts[1]

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	if err != nil {
		log.Fatalf("Failed to create session: %v\n", err)
	}

	svc := s3.New(sess)

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}

	err = svc.ListObjectsV2Pages(params, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			if *obj.StorageClass == "REDUCED_REDUNDANCY" {
				restoreObject(svc, bucketName, *obj.Key)
			}
		}
		return true
	})

	if err != nil {
		log.Printf("Failed to list objects for bucket path %s: %v\n", bucketPath, err)
		return
	}

	err = updateProcessedPaths(requestID, bucketPath)
	if err != nil {
		log.Printf("Failed to update processed paths for Request ID %s: %v\n", requestID, err)
	}
}

func main() {
	bucketPaths := flag.String("bucket_paths", "", "Comma-separated list of S3 bucket paths to restore")
	region := flag.String("region", "", "AWS region")
	ttl := flag.Int("ttl", 30, "Time-to-live (TTL) in days for restored objects before reverting to original storage class")
	flag.Parse()

	if *bucketPaths == "" {
		log.Fatal("Please provide bucket paths")
	}
	if *region == "" {
		log.Fatal("Please provide an AWS region")
	}

	requestID := generateRequestID()
	bucketPathsList := strings.Split(*bucketPaths, ",")

	err := createDBAndRecord(requestID, bucketPathsList, *ttl)
	if err != nil {
		log.Fatalf("Failed to create DB record: %v\n", err)
	}

	for _, path := range bucketPathsList {
		restoreObjectsInPath(path, *region, requestID)
	}

	fmt.Printf("Restore process completed for Request ID: %s\n", requestID)
}
