package layer

import (
	"bytes"
	"context"
	"encoding/csv"
	"github.com/google/uuid"
	cdl "github.com/mimiro-io/common-datalayer"
	"github.com/mimiro-io/entity-graph-data-model"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"os"
	"testing"
	"time"
)

var minioC testcontainers.Container

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Setup MinIO container
	req := testcontainers.ContainerRequest{
		Image:        "minio/minio",
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ACCESS_KEY": "minioadmin",
			"MINIO_SECRET_KEY": "minioadmin",
		},
		Cmd:        []string{"server", "/data"},
		WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000/tcp").WithStartupTimeout(120 * time.Second),
	}

	var err error
	minioC, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("could not start container: %s", err)
	}
	defer func() {
		minioC.Terminate(ctx)
	}()

	// Run tests
	code := m.Run()

	os.Exit(code)
}

func TestStartStopFileSystemDataLayer(t *testing.T) {
	configLocation := "./config"
	serviceRunner := cdl.NewServiceRunner(NewS3DataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *cdl.Config) error {
		config.NativeSystemConfig["path"] = "/tmp"
		return nil
	})
	err := serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}

func writeSampleCsv(bucket string, objectKey string) error {
	client, err := MakeS3TestClient()
	if err != nil {
		return err
	}

	var buf bytes.Buffer

	// Create a new CSV writer using the buffer
	writer := csv.NewWriter(&buf)
	writer.Write([]string{"id", "name", "age", "worksfor"})
	writer.Write([]string{"1", "John", "30", "Mimiro"})
	writer.Write([]string{"2", "Jane", "25", "Mimiro"})
	writer.Write([]string{"3", "Jim", "35", "Mimiro"})
	writer.Flush()

	if err := writer.Error(); err != nil {
		return err
	}

	// Write the buffer contents to the S3 bucket
	_, err = client.PutObject(context.Background(), bucket, objectKey, &buf, int64(buf.Len()), minio.PutObjectOptions{ContentType: "text/csv"})
	if err != nil {
		return err
	}

	return nil
}

func TestGetChanges(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	client, err := MakeS3TestClient()
	if err != nil {
		t.Error(err)
	}

	bucket := "test-" + guid
	err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: "eu-west-1"})
	if err != nil {
		t.Error(err)
	}

	defer func() {
		// remove all objects in the bucket
		objectsCh := client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{})
		for object := range objectsCh {
			if object.Err != nil {
				t.Error(object.Err)
			}

			err = client.RemoveObject(context.Background(), bucket, object.Key, minio.RemoveObjectOptions{})
			if err != nil {
				t.Error(err)
			}
		}

		err = client.RemoveBucket(context.Background(), bucket)
		if err != nil {
			t.Error(err)
		}
	}()

	// create some data
	err = writeSampleCsv(bucket, "data.csv")
	if err != nil {
		t.Error(err)
	}

	configLocation := "./config"
	serviceRunner := cdl.NewServiceRunner(NewS3DataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *cdl.Config) error {
		config.NativeSystemConfig["secure"] = false
		config.NativeSystemConfig["access_key"] = "minioadmin"
		config.NativeSystemConfig["secret"] = "minioadmin"
		endpoint, err := minioC.Endpoint(context.Background(), "")
		if err != nil {
			return err
		}
		config.NativeSystemConfig["endpoint"] = endpoint

		// get dataset definition with name people
		for _, ds := range config.DatasetDefinitions {
			if ds.DatasetName == "people" {
				ds.SourceConfig["bucket"] = bucket
			}
		}

		return nil
	})

	err = serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	changes, err := ds.Changes("", 0, false)
	if err != nil {
		t.Error(err)
	}

	entity, err := changes.Next()
	if err != nil {
		t.Error(err)
	}

	if entity == nil {
		t.Error("Expected entity")
	}

	if entity.ID != "http://data.sample.org/things/1" {
		t.Error("Expected 1")
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}

func TestMultiSourceFilesGetChanges(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	client, err := MakeS3TestClient()
	if err != nil {
		t.Error(err)
	}

	bucket := "test-" + guid
	err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: "eu-west-1"})
	if err != nil {
		t.Error(err)
	}

	defer func() {
		// remove all objects in the bucket
		objectsCh := client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{})
		for object := range objectsCh {
			if object.Err != nil {
				t.Error(object.Err)
			}

			err = client.RemoveObject(context.Background(), bucket, object.Key, minio.RemoveObjectOptions{})
			if err != nil {
				t.Error(err)
			}
		}

		err = client.RemoveBucket(context.Background(), bucket)
		if err != nil {
			t.Error(err)
		}
	}()

	// create some data
	err = writeSampleCsv(bucket, "data1.csv")
	if err != nil {
		t.Error(err)
	}

	err = writeSampleCsv(bucket, "data2.csv")
	if err != nil {
		t.Error(err)
	}

	err = writeSampleCsv(bucket, "data3.txt")
	if err != nil {
		t.Error(err)
	}

	configLocation := "./config"
	serviceRunner := cdl.NewServiceRunner(NewS3DataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *cdl.Config) error {
		config.NativeSystemConfig["secure"] = false
		config.NativeSystemConfig["access_key"] = "minioadmin"
		config.NativeSystemConfig["secret"] = "minioadmin"
		endpoint, err := minioC.Endpoint(context.Background(), "")
		if err != nil {
			return err
		}
		config.NativeSystemConfig["endpoint"] = endpoint

		// get dataset definition with name people
		for _, ds := range config.DatasetDefinitions {
			if ds.DatasetName == "people" {
				ds.SourceConfig["bucket"] = bucket
			}
		}

		return nil
	})

	err = serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	changes, err := ds.Changes("", 0, false)
	if err != nil {
		t.Error(err)
	}

	count := 0

	// iterate next until no more
	for {
		entity, err := changes.Next()
		if err != nil {
			t.Error(err)
		}

		if entity == nil {
			break
		}

		count++
	}

	if count != 6 {
		t.Error("Expected 6 but got ", count)
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}

func TestMultiSourceFilesInFolderGetChanges(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	client, err := MakeS3TestClient()
	if err != nil {
		t.Error(err)
	}

	bucket := "test-" + guid
	err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: "eu-west-1"})
	if err != nil {
		t.Error(err)
	}

	defer func() {
		// remove all objects in the bucket
		objectsCh := client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{})
		for object := range objectsCh {
			if object.Err != nil {
				t.Error(object.Err)
			}

			err = client.RemoveObject(context.Background(), bucket, object.Key, minio.RemoveObjectOptions{})
			if err != nil {
				t.Error(err)
			}
		}

		err = client.RemoveBucket(context.Background(), bucket)
		if err != nil {
			t.Error(err)
		}
	}()

	// create some data
	err = writeSampleCsv(bucket, "data1.csv")
	if err != nil {
		t.Error(err)
	}

	// create child folder
	err = writeSampleCsv(bucket, "data2.csv")
	if err != nil {
		t.Error(err)
	}

	err = writeSampleCsv(bucket, "data3.csv")
	if err != nil {
		t.Error(err)
	}

	configLocation := "./config"
	serviceRunner := cdl.NewServiceRunner(NewS3DataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *cdl.Config) error {
		config.NativeSystemConfig["secure"] = false
		config.NativeSystemConfig["access_key"] = "minioadmin"
		config.NativeSystemConfig["secret"] = "minioadmin"
		endpoint, err := minioC.Endpoint(context.Background(), "")
		if err != nil {
			return err
		}
		config.NativeSystemConfig["endpoint"] = endpoint

		// get dataset definition with name people
		for _, ds := range config.DatasetDefinitions {
			if ds.DatasetName == "people" {
				ds.SourceConfig["bucket"] = bucket
			}
		}

		return nil
	})

	err = serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	changes, err := ds.Changes("", 0, false)
	if err != nil {
		t.Error(err)
	}

	count := 0

	// iterate next until no more
	for {
		entity, err := changes.Next()
		if err != nil {
			t.Error(err)
		}

		if entity == nil {
			break
		}

		count++
	}

	if count != 9 {
		t.Error("Expected 6")
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}

// create a test for GetChanges with a since filter. write files check timestamp of last and use that.
// expect no rsults
func TestGetChangesWithSinceFilter(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	client, err := MakeS3TestClient()
	if err != nil {
		t.Error(err)
	}

	bucket := "test-" + guid
	err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: "eu-west-1"})
	if err != nil {
		t.Error(err)
	}

	defer func() {
		// remove all objects in the bucket
		objectsCh := client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{})
		for object := range objectsCh {
			if object.Err != nil {
				t.Error(object.Err)
			}

			err = client.RemoveObject(context.Background(), bucket, object.Key, minio.RemoveObjectOptions{})
			if err != nil {
				t.Error(err)
			}
		}

		err = client.RemoveBucket(context.Background(), bucket)
		if err != nil {
			t.Error(err)
		}
	}()

	// create some data
	err = writeSampleCsv(bucket, "data.csv")
	if err != nil {
		t.Error(err)
	}

	configLocation := "./config"
	serviceRunner := cdl.NewServiceRunner(NewS3DataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *cdl.Config) error {
		config.NativeSystemConfig["secure"] = false
		config.NativeSystemConfig["access_key"] = "minioadmin"
		config.NativeSystemConfig["secret"] = "minioadmin"
		endpoint, err := minioC.Endpoint(context.Background(), "")
		if err != nil {
			return err
		}
		config.NativeSystemConfig["endpoint"] = endpoint

		// get dataset definition with name people
		for _, ds := range config.DatasetDefinitions {
			if ds.DatasetName == "people" {
				ds.SourceConfig["bucket"] = bucket
			}
		}

		return nil
	})

	err = serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	changes, err := ds.Changes("", 0, false)
	if err != nil {
		t.Error(err)
	}

	// iterate next until no more
	for {
		entity, err := changes.Next()
		if err != nil {
			t.Error(err)
		}

		if entity == nil {
			break
		}
	}

	token, err := changes.Token()
	if err != nil {
		t.Error(err)
	}

	changes, err = ds.Changes(token.Token, 0, false)
	if err != nil {
		t.Error(err)
	}

	count := 0

	// iterate next until no more
	for {
		entity, err := changes.Next()
		if err != nil {
			t.Error(err)
		}

		if entity == nil {
			break
		}

		count++
	}

	if count != 0 {
		t.Errorf("Expected 0 but got %v", count)
	}

	time.Sleep(10 * time.Millisecond)

	// write another file
	err = writeSampleCsv(bucket, "data2.csv")
	if err != nil {
		t.Error(err)
	}

	changes, err = ds.Changes(token.Token, 0, false)
	if err != nil {
		t.Error(err)

	}

	count = 0

	// iterate next until no more
	for {
		entity, err := changes.Next()
		if err != nil {
			t.Error(err)
		}

		if entity == nil {
			break
		}

		count++
	}

	if count != 3 {
		t.Error("Expected 3")
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}

}

// write the same tests as above but for GetEntities
func TestGetEntities(t *testing.T) {
	// make a guid for test folder name
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	client, err := MakeS3TestClient()
	if err != nil {
		t.Error(err)
	}

	bucket := "test-" + guid
	err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: "eu-west-1"})
	if err != nil {
		t.Error(err)
	}

	defer func() {
		// remove all objects in the bucket
		objectsCh := client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{})
		for object := range objectsCh {
			if object.Err != nil {
				t.Error(object.Err)
			}

			err = client.RemoveObject(context.Background(), bucket, object.Key, minio.RemoveObjectOptions{})
			if err != nil {
				t.Error(err)
			}
		}

		err = client.RemoveBucket(context.Background(), bucket)
		if err != nil {
			t.Error(err)
		}
	}()

	// create some data
	err = writeSampleCsv(bucket, "data.csv")
	if err != nil {
		t.Error(err)
	}

	configLocation := "./config"
	serviceRunner := cdl.NewServiceRunner(NewS3DataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *cdl.Config) error {
		config.NativeSystemConfig["secure"] = false
		config.NativeSystemConfig["access_key"] = "minioadmin"
		config.NativeSystemConfig["secret"] = "minioadmin"
		endpoint, err := minioC.Endpoint(context.Background(), "")
		if err != nil {
			return err
		}
		config.NativeSystemConfig["endpoint"] = endpoint

		// get dataset definition with name people
		for _, ds := range config.DatasetDefinitions {
			if ds.DatasetName == "people" {
				ds.SourceConfig["bucket"] = bucket
			}
		}

		return nil
	})

	err = serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	_, err = ds.Entities("", 0)
	if err == nil {
		t.Error("Expected error")
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}

func MakeS3TestClient() (*minio.Client, error) {

	ctx := context.Background()
	endpoint, err := minioC.Endpoint(ctx, "")
	if err != nil {
		log.Fatalf("could not get container endpoint: %s", err)
	}

	// Initialize MinIO client
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		Secure: false,
	})
	if err != nil {
		log.Fatalf("could not create minio client: %s", err)
	}

	return minioClient, nil

	// env get access key
	// env get access key
	/* accessKey := os.Getenv("AWS_ACCESS_KEY_ID")

	// env get secret key
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	s3Client, err := minio.New("s3.amazonaws.com", &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: true,
	})

	if err != nil {
		return nil, err
	}

	return s3Client, nil */
}

func TestConnect(t *testing.T) {
	s3Client, err := MakeS3TestClient()
	if err != nil {
		t.Error(err)
	}

	// list files
	_, err = s3Client.ListBuckets(context.Background())
	if err != nil {
		t.Error(err)
	}

	// guid for new bucket name
	guid := "b-" + uuid.New().String()
	err = s3Client.MakeBucket(context.Background(), guid, minio.MakeBucketOptions{Region: "eu-west-1"})
	if err != nil {
		t.Error(err)
	}

	// delete bucket
	err = s3Client.RemoveBucket(context.Background(), guid)
	if err != nil {
		t.Error(err)
	}
}

// test write full sync
func TestWriteFullSync(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	client, err := MakeS3TestClient()
	if err != nil {
		t.Error(err)
	}

	bucket := "test-" + guid
	err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: "eu-west-1"})
	if err != nil {
		t.Error(err)
	}

	defer func() {
		// remove all objects in the bucket
		objectsCh := client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{})
		for object := range objectsCh {
			if object.Err != nil {
				t.Error(object.Err)
			}

			err = client.RemoveObject(context.Background(), bucket, object.Key, minio.RemoveObjectOptions{})
			if err != nil {
				t.Error(err)
			}
		}

		err = client.RemoveBucket(context.Background(), bucket)
		if err != nil {
			t.Error(err)
		}
	}()

	configLocation := "./config"
	serviceRunner := cdl.NewServiceRunner(NewS3DataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *cdl.Config) error {
		config.NativeSystemConfig["secure"] = false
		config.NativeSystemConfig["access_key"] = "minioadmin"
		config.NativeSystemConfig["secret"] = "minioadmin"
		endpoint, err := minioC.Endpoint(context.Background(), "")
		if err != nil {
			return err
		}
		config.NativeSystemConfig["endpoint"] = endpoint

		// get dataset definition with name people
		for _, ds := range config.DatasetDefinitions {
			if ds.DatasetName == "people" {
				ds.SourceConfig["bucket"] = bucket
			}
		}

		return nil
	})

	err = serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	// write entities
	guidsyncId := uuid.New().String()

	batch := cdl.BatchInfo{SyncId: guidsyncId, IsLastBatch: true, IsStartBatch: true}
	writer, err := ds.FullSync(context.Background(), batch)
	if err != nil {
		t.Error(err)
	}

	entity := makeEntity("1")
	err = writer.Write(entity)
	if err != nil {
		t.Error(err)
	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	objectsCh := client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{})
	for object := range objectsCh {
		if object.Key == "people.csv" {
			if object.Size == 0 {
				t.Error("Expected file to have content")
			}
		}
	}

	// try reading them back with changes
	changes, err := ds.Changes("", 0, false)
	if err != nil {
		t.Error(err)
	}

	entity, err = changes.Next()
	if err != nil {
		t.Error(err)
	}

	if entity == nil {
		t.Error("Expected entity")
	}

	if entity.ID != "http://data.sample.org/things/1" {
		t.Error("Expected 1")
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}

func TestWriteIncrementalSyncAppend(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	client, err := MakeS3TestClient()
	if err != nil {
		t.Error(err)
	}

	bucket := "test-" + guid
	err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: "eu-west-1"})
	if err != nil {
		t.Error(err)
	}

	defer func() {
		// remove all objects in the bucket
		objectsCh := client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{})
		for object := range objectsCh {
			if object.Err != nil {
				t.Error(object.Err)
			}

			err = client.RemoveObject(context.Background(), bucket, object.Key, minio.RemoveObjectOptions{})
			if err != nil {
				t.Error(err)
			}
		}

		err = client.RemoveBucket(context.Background(), bucket)
		if err != nil {
			t.Error(err)
		}
	}()

	configLocation := "./config"
	serviceRunner := cdl.NewServiceRunner(NewS3DataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *cdl.Config) error {
		config.NativeSystemConfig["secure"] = false
		config.NativeSystemConfig["access_key"] = "minioadmin"
		config.NativeSystemConfig["secret"] = "minioadmin"
		endpoint, err := minioC.Endpoint(context.Background(), "")
		if err != nil {
			return err
		}
		config.NativeSystemConfig["endpoint"] = endpoint

		// get dataset definition with name people
		for _, ds := range config.DatasetDefinitions {
			if ds.DatasetName == "people" {
				ds.SourceConfig["bucket"] = bucket
			}
		}

		return nil
	})

	err = serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	// write entities
	writer, err := ds.Incremental(context.Background())
	if err != nil {
		t.Error(err)
	}

	entity := makeEntity("1")
	err = writer.Write(entity)
	if err != nil {
		t.Error(err)
	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	// try reading them back with changes
	changes, err := ds.Changes("", 0, false)
	if err != nil {
		t.Error(err)
	}

	entity, err = changes.Next()
	if err != nil {
		t.Error(err)
	}

	if entity == nil {
		t.Error("Expected entity")
	}

	if entity.ID != "http://data.sample.org/things/1" {
		t.Error("Expected 1")
	}

	// write some more
	writer, err = ds.Incremental(context.Background())
	if err != nil {
		t.Error(err)

	}

	entity = makeEntity("2")
	err = writer.Write(entity)
	if err != nil {
		t.Error(err)

	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	// try reading them back with changes
	changes, err = ds.Changes("", 0, false)
	if err != nil {
		t.Error(err)
	}

	entity, err = changes.Next()
	if err != nil {
		t.Error(err)
	}

	if entity == nil {
		t.Error("Expected entity")
	}

	entity, err = changes.Next()
	if err != nil {
		t.Error(err)
	}

	if entity == nil {
		t.Error("Expected entity")
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}

func TestWriteIncrementalSyncNewFilePerBatch(t *testing.T) {
	// make a guid for test folder name
	guid := uuid.New().String()

	// create temp folder
	client, err := MakeS3TestClient()
	if err != nil {
		t.Error(err)
	}

	bucket := "test-" + guid
	err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: "eu-west-1"})
	if err != nil {
		t.Error(err)
	}

	defer func() {
		// remove all objects in the bucket
		objectsCh := client.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{})
		for object := range objectsCh {
			if object.Err != nil {
				t.Error(object.Err)
			}

			err = client.RemoveObject(context.Background(), bucket, object.Key, minio.RemoveObjectOptions{})
			if err != nil {
				t.Error(err)
			}
		}

		err = client.RemoveBucket(context.Background(), bucket)
		if err != nil {
			t.Error(err)
		}
	}()

	configLocation := "./config"
	serviceRunner := cdl.NewServiceRunner(NewS3DataLayer)
	serviceRunner.WithConfigLocation(configLocation)
	serviceRunner.WithEnrichConfig(func(config *cdl.Config) error {
		config.NativeSystemConfig["secure"] = false
		config.NativeSystemConfig["access_key"] = "minioadmin"
		config.NativeSystemConfig["secret"] = "minioadmin"
		endpoint, err := minioC.Endpoint(context.Background(), "")
		if err != nil {
			return err
		}
		config.NativeSystemConfig["endpoint"] = endpoint

		// get dataset definition with name people
		for _, ds := range config.DatasetDefinitions {
			if ds.DatasetName == "people" {
				ds.SourceConfig["bucket"] = bucket
			}
		}

		return nil
	})

	err = serviceRunner.Start()
	if err != nil {
		t.Error(err)
	}

	service := serviceRunner.LayerService()
	ds, err := service.Dataset("people")
	if err != nil {
		t.Error(err)
	}

	// write entities
	writer, err := ds.Incremental(context.Background())
	if err != nil {
		t.Error(err)
	}

	entity := makeEntity("1")
	err = writer.Write(entity)
	if err != nil {
		t.Error(err)
	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	// try reading them back with changes
	changes, err := ds.Changes("", 0, false)
	if err != nil {
		t.Error(err)
	}

	entity, err = changes.Next()
	if err != nil {
		t.Error(err)
	}

	if entity == nil {
		t.Error("Expected entity")
	}

	if entity.ID != "http://data.sample.org/things/1" {
		t.Error("Expected 1")
	}

	// write some more
	writer, err = ds.Incremental(context.Background())
	if err != nil {
		t.Error(err)

	}

	entity = makeEntity("2")
	err = writer.Write(entity)
	if err != nil {
		t.Error(err)

	}

	err = writer.Close()
	if err != nil {
		t.Error(err)
	}

	// try reading them back with changes
	changes, err = ds.Changes("", 0, false)
	if err != nil {
		t.Error(err)
	}

	entity, err = changes.Next()
	if err != nil {
		t.Error(err)
	}

	if entity == nil {
		t.Error("Expected entity")
	}

	entity, err = changes.Next()
	if err != nil {
		t.Error(err)
	}

	if entity == nil {
		t.Error("Expected entity")
	}

	err = serviceRunner.Stop()
	if err != nil {
		t.Error(err)
	}
}

func makeEntity(id string) *egdm.Entity {
	entity := egdm.NewEntity().SetID("http://data.sample.org/things/" + id)
	entity.SetProperty("http://data.sample.org/name", "brian")
	entity.SetProperty("http://data.sample.org/age", 23)
	entity.SetReference("http://data.sample.org/worksfor", "http://data.sample.org/things/worksfor/mimiro")
	return entity
}
