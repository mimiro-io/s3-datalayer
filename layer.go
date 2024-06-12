package layer

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	cdl "github.com/mimiro-io/common-datalayer"
	"github.com/mimiro-io/common-datalayer/encoder"
	egdm "github.com/mimiro-io/entity-graph-data-model"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type S3DataLayer struct {
	config   *cdl.Config
	s3config *S3DataLayerConfig
	logger   cdl.Logger
	metrics  cdl.Metrics
	datasets map[string]*S3Dataset
}

func EnrichConfig(config *cdl.Config) error {
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if accessKey != "" {
		config.NativeSystemConfig["access_key"] = accessKey
	}

	if secretKey != "" {
		config.NativeSystemConfig["secret"] = secretKey
	}

	return nil
}

func MakeS3Client(endpoint string, accessKey string, secretKey string) (*minio.Client, error) {
	s3Client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: true,
	})
	if err != nil {
		return nil, err
	}
	return s3Client, nil
}

type S3DataLayerConfig struct {
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"access_key"` // AWS_ACCESS_KEY_ID
	SecretKey string `json:"secret"`     // AWS_SECRET_ACCESS_KEY
}

func NewS3DataLayer(conf *cdl.Config, logger cdl.Logger, metrics cdl.Metrics) (cdl.DataLayerService, error) {
	datalayer := &S3DataLayer{config: conf, logger: logger, metrics: metrics}

	err := datalayer.UpdateConfiguration(conf)
	if err != nil {
		return nil, err
	}

	return datalayer, nil
}

func (dl *S3DataLayer) Stop(ctx context.Context) error {
	// noop
	return nil
}

func (dl *S3DataLayer) UpdateConfiguration(config *cdl.Config) cdl.LayerError {
	dl.config = config

	// get the native system config as s3 layer config
	s3Config := &S3DataLayerConfig{}
	nativeConfig := config.NativeSystemConfig
	configJson, _ := json.Marshal(nativeConfig)
	err := json.Unmarshal(configJson, s3Config)

	if err != nil {
		return cdl.Err(fmt.Errorf("could not unmarshal native system config because %s", err.Error()), cdl.LayerErrorInternal)
	}

	dl.s3config = s3Config

	// make all s3 datasets
	dl.datasets = make(map[string]*S3Dataset)

	for _, definition := range config.DatasetDefinitions {
		dl.datasets[definition.DatasetName], err = NewS3Dataset(definition.DatasetName, s3Config, definition, dl.logger)
		if err != nil {
			dl.logger.Error("could not create dataset %s because %s", definition.DatasetName, err.Error())
		}
	}

	return nil
}

func (dl *S3DataLayer) Dataset(dataset string) (cdl.Dataset, cdl.LayerError) {
	ds, ok := dl.datasets[dataset]
	if !ok {
		return nil, cdl.Err(fmt.Errorf("dataset %s not found", dataset), cdl.LayerErrorBadParameter)
	}

	return ds, nil
}

func (dl *S3DataLayer) DatasetDescriptions() []*cdl.DatasetDescription {
	var datasetDescriptions []*cdl.DatasetDescription

	// iterate over the datasest config and create one for each
	for key := range dl.datasets {
		datasetDescriptions = append(datasetDescriptions, &cdl.DatasetDescription{Name: key})
	}

	return datasetDescriptions
}

func NewS3DatasetConfig(datasetName string, sourceConfig map[string]any) (*S3DatasetConfig, error) {
	data, err := json.Marshal(sourceConfig)
	if err != nil {
		return nil, err
	}

	config := &S3DatasetConfig{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	// check bucket not empty
	if config.Bucket == "" {
		return nil, fmt.Errorf("no bucket specified in source config")
	}

	// file pattern
	if config.ReadFilePattern == "" {
		// * with the encoding
		config.ReadFilePattern = fmt.Sprintf("*.%s", config.Encoding)
	}

	// write full sync file name
	if config.WriteFullSyncFileName == "" {
		// use all but append encoding
		config.WriteFullSyncFileName = fmt.Sprintf("alldata.%s", config.Encoding)
	}

	// write incremental file name
	if config.WriteIncrementalFileName == "" {
		// use dataset name and append encoding
		config.WriteIncrementalFileName = fmt.Sprintf("%s.%s", datasetName, config.Encoding)
	}

	return config, nil
}

type S3DatasetConfig struct {
	Encoding                    string `json:"encoding"`
	Bucket                      string `json:"bucket"`
	Region                      string `json:"region"`
	ReadFilePattern             string `json:"read_file_pattern"`
	SupportSinceByFileTimestamp bool   `json:"support_since_by_file_timestamp"`
	WriteFullSyncFileName       string `json:"write_full_sync_file"`
	WriteIncrementalFileName    string `json:"write_incremental_file"`
}

func NewS3Dataset(name string, layerConfig *S3DataLayerConfig, datasetDefinition *cdl.DatasetDefinition, logger cdl.Logger) (*S3Dataset, error) {
	sourceConfig := datasetDefinition.SourceConfig

	_, ok := sourceConfig["encoding"].(string)
	if !ok {
		return nil, fmt.Errorf("no encoding specified in source config")
	}

	config, err := NewS3DatasetConfig(name, sourceConfig)
	if err != nil {
		return nil, err
	}

	return &S3Dataset{name: name,
		layerConfig:       layerConfig,
		config:            config,
		datasetDefinition: datasetDefinition,
		logger:            logger}, nil
}

type S3Dataset struct {
	logger            cdl.Logger
	name              string                 // dataset name
	datasetDefinition *cdl.DatasetDefinition // the dataset definition with mappings etc
	config            *S3DatasetConfig       // the dataset config
	layerConfig       *S3DataLayerConfig     // the layer config
}

func (s3dataset *S3Dataset) MetaData() map[string]any {
	return make(map[string]any)
}

func (s3dataset *S3Dataset) Name() string {
	return s3dataset.name
}

func checkBucketExists(client *minio.Client, bucket string) (bool, error) {
	found, err := client.BucketExists(context.Background(), bucket)
	if err != nil {
		return false, err
	}

	return found, nil
}

func (s3dataset *S3Dataset) AssertBucket(bucket string) error {
	// make client
	client, err := MakeS3Client(s3dataset.layerConfig.Endpoint, s3dataset.layerConfig.AccessKey, s3dataset.layerConfig.SecretKey)
	if err != nil {
		return err
	}

	// check if bucket exists
	found, err := client.BucketExists(context.Background(), bucket)
	if err != nil {
		return err
	}

	if !found {
		// check region is defined
		if s3dataset.config.Region == "" {
			return fmt.Errorf("region not defined")
		}

		// create bucket
		err = client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: s3dataset.config.Region})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s3dataset *S3Dataset) FullSync(ctx context.Context, batchInfo cdl.BatchInfo) (cdl.DatasetWriter, cdl.LayerError) {
	var err error

	// assert bucket
	err = s3dataset.AssertBucket(s3dataset.config.Bucket)
	if err != nil {
		return nil, cdl.Err(fmt.Errorf("could not assert bucket because %s", err.Error()), cdl.LayerErrorInternal)
	}

	// need to create a pipe to glue things together
	reader, writer := io.Pipe()

	enc, err := encoder.NewItemWriter(s3dataset.datasetDefinition.SourceConfig, s3dataset.logger, writer, &batchInfo)
	factory, err := encoder.NewItemFactory(s3dataset.datasetDefinition.SourceConfig)
	mapper := cdl.NewMapper(s3dataset.logger, s3dataset.datasetDefinition.IncomingMappingConfig, s3dataset.datasetDefinition.OutgoingMappingConfig)

	// create tmp file for full sync
	tmpFullSyncPath := "fullsyncpart-" + batchInfo.SyncId + "-" + uuid.New().String()
	client, cerr := MakeS3Client(s3dataset.layerConfig.Endpoint, s3dataset.layerConfig.AccessKey, s3dataset.layerConfig.SecretKey)
	if cerr != nil {
		s3dataset.logger.Error("could not create s3 client because %s", cerr.Error())
		return nil, cdl.Err(fmt.Errorf("could not create s3 client because %s", cerr.Error()), cdl.LayerErrorInternal)
	}

	datasetWriter := &S3DatasetWriter{encoding: s3dataset.config.Encoding, writer: writer, syncId: batchInfo.SyncId, s3client: client, logger: s3dataset.logger, enc: enc, mapper: mapper, factory: factory, bucket: s3dataset.config.Bucket, fullSyncFileName: s3dataset.config.WriteFullSyncFileName, closeFullSync: batchInfo.IsLastBatch}
	datasetWriter.wg.Add(1)

	go func() {
		defer datasetWriter.wg.Done()
		userTags := map[string]string{"syncid": batchInfo.SyncId}
		_, cerr = client.PutObject(ctx, s3dataset.config.Bucket, tmpFullSyncPath, reader, -1, minio.PutObjectOptions{ContentType: "text/plain", UserTags: userTags})
		if cerr != nil {
			s3dataset.logger.Error("could not put object because %s", cerr.Error())
			return
		}
	}()

	return datasetWriter, nil
}

func (s3dataset *S3Dataset) Incremental(ctx context.Context) (cdl.DatasetWriter, cdl.LayerError) {
	var err error

	id := uuid.New().String()
	partfileName := fmt.Sprintf("part-%s-%s", id, s3dataset.config.WriteIncrementalFileName)

	err = s3dataset.AssertBucket(s3dataset.config.Bucket)
	if err != nil {
		return nil, cdl.Err(fmt.Errorf("could not assert bucket because %s", err.Error()), cdl.LayerErrorInternal)
	}

	// need to create a pipe to glue things together
	reader, writer := io.Pipe()

	enc, err := encoder.NewItemWriter(s3dataset.datasetDefinition.SourceConfig, s3dataset.logger, writer, nil)
	factory, err := encoder.NewItemFactory(s3dataset.datasetDefinition.SourceConfig)
	mapper := cdl.NewMapper(s3dataset.logger, s3dataset.datasetDefinition.IncomingMappingConfig, s3dataset.datasetDefinition.OutgoingMappingConfig)

	// create tmp file for full sync
	client, cerr := MakeS3Client(s3dataset.layerConfig.Endpoint, s3dataset.layerConfig.AccessKey, s3dataset.layerConfig.SecretKey)
	if cerr != nil {
		s3dataset.logger.Error("could not create s3 client because %s", cerr.Error())
		return nil, cdl.Err(fmt.Errorf("could not create s3 client because %s", cerr.Error()), cdl.LayerErrorInternal)
	}

	datasetWriter := &S3DatasetWriter{encoding: s3dataset.config.Encoding, writer: writer, s3client: client, logger: s3dataset.logger, enc: enc, mapper: mapper, factory: factory, bucket: s3dataset.config.Bucket}
	datasetWriter.wg.Add(1)

	go func() {
		defer datasetWriter.wg.Done()
		_, cerr = client.PutObject(ctx, s3dataset.config.Bucket, partfileName, reader, -1, minio.PutObjectOptions{ContentType: getContentTypeFromEncoding(s3dataset.config.Encoding)})
		if cerr != nil {
			s3dataset.logger.Error("could not put object because %s", cerr.Error())
			return
		}
	}()

	return datasetWriter, nil
}

type S3DatasetWriter struct {
	logger           cdl.Logger
	enc              encoder.ItemWriter
	factory          encoder.ItemFactory
	mapper           *cdl.Mapper
	writer           io.WriteCloser
	s3client         *minio.Client
	bucket           string
	fullSyncFileName string
	closeFullSync    bool
	syncId           string
	wg               sync.WaitGroup
	encoding         string
}

func getContentTypeFromEncoding(encoding string) string {
	switch encoding {
	case "json":
		return "application/json"
	case "csv":
		return "text/csv"
	case "parquet":
		return "application/x-parquet"
	default:
		return "application/octet-stream"
	}
}

func (s3writer *S3DatasetWriter) Write(entity *egdm.Entity) cdl.LayerError {
	item := s3writer.factory.NewItem()
	err := s3writer.mapper.MapEntityToItem(entity, item)
	if err != nil {
		return cdl.Err(fmt.Errorf("could not map entity to item because %s", err.Error()), cdl.LayerErrorInternal)
	}

	err = s3writer.enc.Write(item)
	if err != nil {
		return cdl.Err(fmt.Errorf("could not write item to file because %s", err.Error()), cdl.LayerErrorInternal)
	}

	return nil
}

func (s3writer *S3DatasetWriter) Close() cdl.LayerError {
	err := s3writer.enc.Close()
	if err != nil {
		return cdl.Err(fmt.Errorf("could not close file because %s", err.Error()), cdl.LayerErrorInternal)
	}

	err = s3writer.writer.Close()
	if err != nil {
		return cdl.Err(fmt.Errorf("could not close file because %s", err.Error()), cdl.LayerErrorInternal)
	}

	// wait for writes to finish
	s3writer.wg.Wait()

	if s3writer.closeFullSync {
		// get a list of files in the bucket that match the sync id of a partfile
		objects, err := listAndFilterObjects(s3writer.s3client, s3writer.bucket, s3writer.syncId)
		if err != nil {
			log.Fatalln(err)
		}

		// Sort objects by Last Modified date, oldest first
		sort.Slice(objects, func(i, j int) bool {
			return objects[i].LastModified.Before(objects[j].LastModified)
		})

		err = combineFilesIntoNewObject(context.Background(), s3writer.logger, s3writer.s3client, s3writer.bucket, objects, s3writer.fullSyncFileName, getContentTypeFromEncoding(s3writer.encoding))
	}

	return nil
}

func combineFilesIntoNewObject(ctx context.Context, logger cdl.Logger, client *minio.Client, bucketName string, objects []ObjectInfo, newObjectName string, contentType string) error {
	reader, writer := io.Pipe()

	// Goroutine to handle writing to the pipe
	go func() {
		defer writer.Close()
		for _, obj := range objects {
			// Get the object from the bucket
			object, err := client.GetObject(ctx, bucketName, obj.Key, minio.GetObjectOptions{})
			if err != nil {
				logger.Error("Error getting object %s from bucket %s: %v", obj, bucketName, err)
				return
			}
			defer object.Close()

			// Write the object data to the pipe
			if _, err := io.Copy(writer, object); err != nil {
				logger.Error("Error copying object %s to pipe: %v", obj, err)
				return
			}
		}
	}()

	// Use the reader part of the pipe to put a new object
	_, err := client.PutObject(ctx, bucketName, newObjectName, reader, -1, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		logger.Error("Error putting object %s in bucket %s: %v", newObjectName, bucketName, err)
		return err
	}

	// delete all part files
	for _, obj := range objects {
		err := client.RemoveObject(ctx, bucketName, obj.Key, minio.RemoveObjectOptions{})
		if err != nil {
			logger.Error("Error removing object %s from bucket %s: %v", obj, bucketName, err)
		}
	}

	return nil
}

type ObjectInfo struct {
	Key          string
	LastModified time.Time
}

func listAndFilterObjects(client *minio.Client, bucketName string, syncId string) ([]ObjectInfo, error) {
	ctx := context.Background()

	// Create a done channel to control the ListObjects goroutine
	objectCh := client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Recursive: false,
	})

	var objects []ObjectInfo
	for object := range objectCh {
		if object.Err != nil {
			return nil, object.Err
		}

		if strings.HasPrefix(object.Key, "fullsyncpart") {
			tags, err := client.GetObjectTagging(context.Background(), bucketName, object.Key, minio.GetObjectTaggingOptions{})
			if err != nil {
				log.Println("Error getting object tags:", err)
				continue
			}

			// get the sync id
			fileSyncId := tags.ToMap()["syncid"]
			if fileSyncId == syncId {
				objects = append(objects, ObjectInfo{
					Key:          object.Key,
					LastModified: object.LastModified,
				})
			}
		}
	}

	return objects, nil
}

func (s3dataset *S3Dataset) Changes(since string, limit int, latestOnly bool) (cdl.EntityIterator, cdl.LayerError) {

	if latestOnly {
		return nil, cdl.Err(fmt.Errorf("latest only operation not supported"), cdl.LayerNotSupported)
	}

	// make client
	client, err := MakeS3Client(s3dataset.layerConfig.Endpoint, s3dataset.layerConfig.AccessKey, s3dataset.layerConfig.SecretKey)
	if err != nil {
		return nil, cdl.Err(fmt.Errorf("could not create s3 client because %s", err.Error()), cdl.LayerErrorInternal)
	}

	// check if bucket exists
	exists, err := checkBucketExists(client, s3dataset.config.Bucket)
	if err != nil {
		return nil, cdl.Err(fmt.Errorf("could not check bucket because %s", err.Error()), cdl.LayerErrorInternal)
	}

	if !exists {
		return nil, cdl.Err(fmt.Errorf("bucket %s does not exist", s3dataset.config.Bucket), cdl.LayerErrorBadParameter)
	}

	// get all object infos
	var objectInfos []ObjectInfo

	// list all objects in the bucket
	objectCh := client.ListObjects(context.Background(), s3dataset.config.Bucket, minio.ListObjectsOptions{Recursive: true})
	for object := range objectCh {
		if object.Err != nil {
			return nil, cdl.Err(fmt.Errorf("could not list objects because %s", object.Err.Error()), cdl.LayerErrorInternal)
		}

		object := ObjectInfo{Key: object.Key, LastModified: object.LastModified}
		objectInfos = append(objectInfos, object)
	}

	var filteredObjects []ObjectInfo
	for _, object := range objectInfos {
		objectName := object.Key
		isMatch, err := filepath.Match(s3dataset.config.ReadFilePattern, objectName)
		if err != nil {
			return nil, cdl.Err(fmt.Errorf("could not match file pattern %s", s3dataset.config.ReadFilePattern), cdl.LayerErrorInternal)
		}

		if isMatch {
			if s3dataset.config.SupportSinceByFileTimestamp && since != "" {
				fileModTime := object.LastModified.UnixMicro()
				sinceTimeAsInt, err := strconv.ParseInt(since, 10, 64)
				if err != nil {
					return nil, cdl.Err(fmt.Errorf("could not parse since time %s", since), cdl.LayerErrorInternal)
				}

				if sinceTimeAsInt < fileModTime {
					filteredObjects = append(filteredObjects, object)
				}
			} else {
				filteredObjects = append(filteredObjects, object)
			}
		}
	}

	// order the files by last modified
	if len(filteredObjects) > 0 {
		sort.Slice(filteredObjects, func(i, j int) bool {
			f1 := filteredObjects[i].LastModified
			f2 := filteredObjects[j].LastModified
			return f1.Before(f2)
		})
	}

	mapper := cdl.NewMapper(s3dataset.logger, nil, s3dataset.datasetDefinition.OutgoingMappingConfig)
	iterator := NewObjectCollectionEntityIterator(client, s3dataset.config.Bucket, s3dataset.datasetDefinition.SourceConfig, filteredObjects, mapper, s3dataset.logger)
	return iterator, nil
}

func (s3dataset *S3Dataset) Entities(from string, limit int) (cdl.EntityIterator, cdl.LayerError) {
	return nil, cdl.Err(fmt.Errorf("operation not supported"), cdl.LayerNotSupported)
}

func NewObjectCollectionEntityIterator(s3client *minio.Client, bucket string, sourceConfig map[string]any, objects []ObjectInfo, mapper *cdl.Mapper, logger cdl.Logger) *ObjectCollectionEntityIterator {
	return &ObjectCollectionEntityIterator{s3client: s3client, bucket: bucket, sourceConfig: sourceConfig, mapper: mapper, objects: objects, objectIndex: 0, logger: logger}
}

type ObjectCollectionEntityIterator struct {
	mapper            *cdl.Mapper
	token             string
	objects           []ObjectInfo
	objectIndex       int
	currentItemReader encoder.ItemIterator
	sourceConfig      map[string]any
	s3client          *minio.Client
	bucket            string
	logger            cdl.Logger
}

func (oci *ObjectCollectionEntityIterator) Context() *egdm.Context {
	ctx := egdm.NewNamespaceContext()
	return ctx.AsContext()
}

func (oci *ObjectCollectionEntityIterator) Next() (*egdm.Entity, cdl.LayerError) {
	if oci.currentItemReader == nil {
		if oci.objectIndex < len(oci.objects) {
			// initialize the current file entity iterator
			objectInfo := oci.objects[oci.objectIndex]
			oci.token = strconv.FormatInt(objectInfo.LastModified.UnixMicro(), 10)
			itemReader, err := oci.NewItemReadCloser(objectInfo.Key, oci.sourceConfig)
			if err != nil {
				return nil, cdl.Err(fmt.Errorf("could not create item reader for file %s becuase %s", objectInfo.Key, err.Error()), cdl.LayerErrorInternal)
			}

			oci.currentItemReader = itemReader
		} else {
			return nil, nil
		}
	}

	// read the next entity from the current file
	item, err := oci.currentItemReader.Read()
	if err != nil {
		return nil, cdl.Err(fmt.Errorf("could not read item from file because %s", err.Error()), cdl.LayerErrorInternal)
	}

	if item == nil {
		// close the current file and move to the next
		err := oci.currentItemReader.Close()
		if err != nil {
			return nil, cdl.Err(fmt.Errorf("could not close item reader for file because %s", err.Error()), cdl.LayerErrorInternal)
		}
		oci.objectIndex++
		if oci.objectIndex < len(oci.objects) {
			objectInfo := oci.objects[oci.objectIndex]
			oci.token = strconv.FormatInt(objectInfo.LastModified.UnixMicro(), 10)
			itemReader, err := oci.NewItemReadCloser(objectInfo.Key, oci.sourceConfig)
			if err != nil {
				return nil, cdl.Err(fmt.Errorf("could not create item reader for file %s becuase %s", objectInfo.Key, err.Error()), cdl.LayerErrorInternal)
			}

			oci.currentItemReader = itemReader
			item, err = oci.currentItemReader.Read()
			if err != nil {
				return nil, cdl.Err(fmt.Errorf("could not read item from file because %s", err.Error()), cdl.LayerErrorInternal)
			}
		}
	}

	if item == nil {
		return nil, nil
	} else {
		entity := &egdm.Entity{Properties: make(map[string]any)}
		err := oci.mapper.MapItemToEntity(item, entity)
		if err != nil {
			return nil, cdl.Err(fmt.Errorf("could not map item to entity because %s", err.Error()), cdl.LayerErrorInternal)
		}
		return entity, nil
	}
}

func (oci *ObjectCollectionEntityIterator) NewItemReadCloser(objectKey string, sourceConfig map[string]any) (encoder.ItemIterator, error) {
	// get the object
	objectStream, err := oci.s3client.GetObject(context.Background(), oci.bucket, objectKey, minio.GetObjectOptions{})

	// get encoder for the file
	itemReader, err := encoder.NewItemIterator(sourceConfig, nil, objectStream)
	if err != nil {
		return nil, cdl.Err(fmt.Errorf("could not create encoder specified in dataset source config"), cdl.LayerErrorBadParameter)
	}

	return itemReader, nil
}

func (oci *ObjectCollectionEntityIterator) Token() (*egdm.Continuation, cdl.LayerError) {
	cont := egdm.NewContinuation()
	cont.Token = oci.token
	return cont, nil
}

func (oci *ObjectCollectionEntityIterator) Close() cdl.LayerError {
	err := oci.currentItemReader.Close()
	if err != nil {
		return cdl.Err(fmt.Errorf("could not close item reader because %s", err.Error()), cdl.LayerErrorInternal)
	}
	return nil
}
