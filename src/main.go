package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/go-ini/ini"
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

type customCredentialsProvider struct {
	creds *aws.Credentials
	mu    sync.RWMutex
}

func (p *customCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return *p.creds, nil
}

func (p *customCredentialsProvider) UpdateCredentials(newCreds aws.Credentials) {
	p.mu.Lock()
	defer p.mu.Unlock()
	*p.creds = newCreds
}

var (
	messageTimestamp string
	credsProvider    *customCredentialsProvider
)

func generateRequestID() string {
	bytes := make([]byte, 16)
	_, err := rand.Read(bytes)
	if err != nil {
		log.Fatalf("Failed to generate request ID: %v", err)
	}
	return hex.EncodeToString(bytes)
}

func sendSlackNotification(channel, threadTS string, blocks []slack.Block) error {
	slackToken := os.Getenv("SLACK_API_TOKEN")
	if slackToken == "" {
		log.Println("No SLACK_API_TOKEN set. Slack messages will not be sent.")
		return fmt.Errorf("SLACK_API_TOKEN is not set")
	}

	api := slack.New(slackToken)
	opts := []slack.MsgOption{
		slack.MsgOptionBlocks(blocks...),
	}

	if threadTS != "" {
		opts = append(opts, slack.MsgOptionTS(threadTS))
	}

	if messageTimestamp != "" {
		opts = append(opts, slack.MsgOptionUpdate(messageTimestamp))
	}

	_, newTimestamp, err := api.PostMessage(channel, opts...)
	if err != nil {
		log.Printf("Failed to send Slack message: %v\n", err)
		return err
	}

	if messageTimestamp == "" {
		messageTimestamp = newTimestamp
	}

	return nil
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

	blocks := []slack.Block{
		slack.NewHeaderBlock(&slack.TextBlockObject{
			Type: slack.PlainTextType,
			Text: ":memo: Created database record",
		}),
		slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: fmt.Sprintf("*Request ID:* `%s`\n*TTL:* `%d` days\n*Created At:* `%s`\n*Updated At:* `%s`\n",
					requestID, ttl, time.Now().UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339)),
			},
			nil,
			nil,
		),
		slack.NewDividerBlock(),
		slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: "*Bucket Paths:*",
			},
			nil,
			nil,
		),
	}
	for _, path := range bucketPaths {
		blocks = append(blocks, slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: fmt.Sprintf("- `%s`", path),
			},
			nil,
			nil,
		))
	}

	if err := sendSlackNotification(os.Getenv("SLACK_CHANNEL_ID"), os.Getenv("SLACK_THREAD_TS"), blocks); err != nil {
		log.Printf("Error sending Slack notification: %v\n", err)
	}

	log.Println("Created database record:", requestID)
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

	blocks := []slack.Block{
		slack.NewHeaderBlock(&slack.TextBlockObject{
			Type: slack.PlainTextType,
			Text: ":hourglass_flowing_sand: Updated database record",
		}),
		slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: fmt.Sprintf("*Request ID:* `%s`\n*Updated At:* `%s`\n",
					requestID, time.Now().UTC().Format(time.RFC3339)),
			},
			nil,
			nil,
		),
		slack.NewDividerBlock(),
		slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: "*Remaining Bucket Paths:*",
			},
			nil,
			nil,
		),
	}
	for _, path := range bp {
		blocks = append(blocks, slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: fmt.Sprintf("- `%s`", path),
			},
			nil,
			nil,
		))
	}
	blocks = append(blocks, slack.NewDividerBlock(), slack.NewSectionBlock(
		&slack.TextBlockObject{
			Type: slack.MarkdownType,
			Text: "*Processed Paths:*",
		},
		nil,
		nil,
	))
	for _, path := range pp {
		blocks = append(blocks, slack.NewSectionBlock(
			&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: fmt.Sprintf("- `%s`", path),
			},
			nil,
			nil,
		))
	}

	if err := sendSlackNotification(os.Getenv("SLACK_CHANNEL_ID"), os.Getenv("SLACK_THREAD_TS"), blocks); err != nil {
		log.Printf("Error sending Slack notification: %v\n", err)
	}

	log.Println("Updated database record:", requestID)

	if len(bp) == 0 {
		deleteQuery := "DELETE FROM restore_requests WHERE request_id = ?"
		_, err = db.Exec(deleteQuery, requestID)
		if err != nil {
			return fmt.Errorf("failed to delete record: %w", err)
		}
		message := fmt.Sprintf(":white_check_mark: *All paths processed for Request ID:* *%s*. *Record deleted.*\n", requestID)
		if err := sendSlackNotification(os.Getenv("SLACK_CHANNEL_ID"), os.Getenv("SLACK_THREAD_TS"), []slack.Block{
			slack.NewSectionBlock(&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: message,
			}, nil, nil),
		}); err != nil {
			log.Printf("Error sending Slack notification: %v\n", err)
		}
		fmt.Print(message)
	}

	return nil
}

func restoreObject(svc *s3.Client, bucketName, key string) error {
	log.Printf("Attempting to restore object: %s/%s", bucketName, key)

	copyInput := &s3.CopyObjectInput{
		Bucket:       aws.String(bucketName),
		CopySource:   aws.String(fmt.Sprintf("%s/%s", bucketName, key)),
		Key:          aws.String(key),
		StorageClass: "STANDARD",
	}

	_, err := svc.CopyObject(context.TODO(), copyInput)
	if err != nil {
		return fmt.Errorf("failed to restore object %s: %v", key, err)
	}

	// Check if the object storage class was updated successfully
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}
	headOutput, err := svc.HeadObject(context.TODO(), headInput)
	if err != nil {
		return fmt.Errorf("failed to verify storage class for object %s: %v", key, err)
	}

	if headOutput.StorageClass == "" || headOutput.StorageClass != "STANDARD" {
		return fmt.Errorf("storage class for object %s is not STANDARD, it is %v", key, headOutput.StorageClass)
	}

	log.Printf("Object %s restored to STANDARD storage class\n", key)
	return nil
}

func restoreObjectsInPath(bucketPath, region, requestID string, failedPaths *[]string, wg *sync.WaitGroup, ch chan struct{}) {
	defer wg.Done()

	// Acquire a slot
	ch <- struct{}{}
	defer func() { <-ch }()

	log.Printf("Starting to process bucket path: %s\n", bucketPath)
	parts := strings.SplitN(bucketPath, "/", 2)
	if len(parts) < 2 {
		log.Printf("Invalid bucket path: %s\n", bucketPath)
		*failedPaths = append(*failedPaths, bucketPath)
		return
	}
	bucketName, prefix := parts[0], parts[1]

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credsProvider),
	)
	if err != nil {
		log.Printf("Failed to load AWS config: %v\n", err)
		*failedPaths = append(*failedPaths, bucketPath)
		return
	}

	svc := s3.NewFromConfig(cfg)

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}

	paginator := s3.NewListObjectsV2Paginator(svc, params)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("Failed to list objects for bucket path %s: %v\n", bucketPath, err)
			*failedPaths = append(*failedPaths, bucketPath)
			return
		}

		for _, obj := range page.Contents {
			if obj.StorageClass != types.ObjectStorageClassStandard {
				err := restoreObject(svc, bucketName, *obj.Key)
				if err != nil {
					log.Printf("Error restoring object %s: %v\n", *obj.Key, err)
					continue
				}
				// Wait for a few seconds to ensure the object is processed before moving on to the next
				time.Sleep(2 * time.Second)
			}
		}
	}

	err = updateProcessedPaths(requestID, bucketPath)
	if err != nil {
		log.Printf("Failed to update processed paths for Request ID %s: %v\n", requestID, err)
		*failedPaths = append(*failedPaths, bucketPath)
	}
}

func getRoleArnFromProfile(profile string) (string, error) {
	credsFile := filepath.Join(os.Getenv("HOME"), ".aws", "credentials")
	cfg, err := ini.Load(credsFile)
	if err != nil {
		return "", fmt.Errorf("failed to load AWS credentials file: %v", err)
	}

	section, err := cfg.GetSection(profile)
	if err != nil {
		return "", fmt.Errorf("failed to get profile %s: %v", profile, err)
	}

	roleArn, err := section.GetKey("role_arn")
	if err != nil {
		return "", fmt.Errorf("failed to get role_arn from profile %s: %v", profile, err)
	}

	return roleArn.String(), nil
}

func assumeRole(roleArn, region string) (aws.Credentials, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("failed to load AWS config: %v", err)
	}

	stsSvc := sts.NewFromConfig(cfg)

	roleSessionName := fmt.Sprintf("kubiya-agent-s3-restore-%d", time.Now().Unix())
	credsCache := aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(stsSvc, roleArn, func(p *stscreds.AssumeRoleOptions) {
		p.RoleSessionName = roleSessionName
	}))

	creds, err := credsCache.Retrieve(context.TODO())
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("failed to assume role: %v", err)
	}

	return creds, nil
}

func renewCredentials(roleArn, region string) {
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		newCreds, err := assumeRole(roleArn, region)
		if err != nil {
			log.Printf("Failed to renew credentials: %v", err)
			continue
		}

		credsProvider.UpdateCredentials(newCreds)

		log.Println("Successfully renewed credentials")
	}
}

func main() {
	bucketPaths := flag.String("bucket_paths", "", "Comma-separated list of S3 bucket paths to restore")
	region := flag.String("region", "", "AWS region")
	ttl := flag.Int("ttl", 30, "Time-to-live (TTL) in days for restored objects before reverting to original storage class")
	profile := flag.String("profile", "default", "AWS profile to use")
	flag.Parse()

	if *bucketPaths == "" {
		log.Fatal("Please provide bucket paths")
	}
	if *region == "" {
		log.Fatal("Please provide an AWS region")
	}

	roleArn, err := getRoleArnFromProfile(*profile)
	if err != nil {
		log.Fatalf("Failed to get role ARN from profile: %v", err)
	}

	initialCreds, err := assumeRole(roleArn, *region)
	if err != nil {
		log.Fatalf("Failed to assume role: %v", err)
	}

	credsProvider = &customCredentialsProvider{creds: &initialCreds}

	go renewCredentials(roleArn, *region)

	requestID := generateRequestID()
	bucketPathsList := strings.Split(*bucketPaths, ",")
	var failedPaths []string

	err = createDBAndRecord(requestID, bucketPathsList, *ttl)
	if err != nil {
		log.Fatalf("Failed to create DB record: %v\n", err)
	}

	var wg sync.WaitGroup
	ch := make(chan struct{}, 5) // Limit to 5 concurrent routines

	for _, path := range bucketPathsList {
		wg.Add(1)
		go restoreObjectsInPath(path, *region, requestID, &failedPaths, &wg, ch)
	}

	wg.Wait()

	if len(failedPaths) > 0 {
		failedPathsJSON, _ := json.Marshal(failedPaths)
		message := fmt.Sprintf(":x: *The following paths failed to be processed for Request ID:* *%s*\n*Failed Paths:* `%s`\n", requestID, failedPathsJSON)
		if err := sendSlackNotification(os.Getenv("SLACK_CHANNEL_ID"), os.Getenv("SLACK_THREAD_TS"), []slack.Block{
			slack.NewSectionBlock(&slack.TextBlockObject{
				Type: slack.MarkdownType,
				Text: message,
			}, nil, nil),
		}); err != nil {
			log.Printf("Error sending Slack notification for failed paths: %v\n", err)
		}
		log.Println(message)
	}

	fmt.Printf(":white_check_mark: *Restore process completed for Request ID:* *%s*\n", requestID)
}