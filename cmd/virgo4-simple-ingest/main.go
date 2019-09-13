package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var maxPayloadSize = 262144

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up <===", os.Args[ 0 ] )

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	sess, err := session.NewSession( )
	if err != nil {
		log.Fatal( err )
	}

	svc := sqs.New(sess)

	// get the queue URL from the name
	result, err := svc.GetQueueUrl( &sqs.GetQueueUrlInput{
		QueueName: aws.String( cfg.OutQueueName ),
	})

	if err != nil {
		log.Fatal( err )
	}

	queueUrl := result.QueueUrl

	file, err := os.Open( cfg.FileName )
	if err != nil {
		log.Fatal( err )
	}
	defer file.Close( )

	reader := bufio.NewReader( file )

	batch_size := 10
	batch_length := 0
	batch := make( []string, 0 )

	count := 0
	start := time.Now()

	for {

		line, err := reader.ReadString( '\n' )

		if err != nil {
			// are we done
			if err == io.EOF {
				break
			} else {
				log.Fatal(err)
			}
		}

		sz := len( line )
		if sz >= maxPayloadSize {
			log.Printf("Ignoring record %d as too large (%d characters)", count, sz )
			continue
		}

		count ++

		// ensure we do not exceed the maximum payload size
		if batch_length + sz < maxPayloadSize {
			batch = append( batch, line )
			batch_length += sz
		} else {

			log.Printf("Sending short batch (%d items)", len( batch ) )

			err := sendMessages( cfg, svc, queueUrl, batch )
			if err != nil {
				log.Fatal( err )
			}

			// reset the batch
			batch = batch[:0]
			batch_length = 0

			// and add the record we just read
			batch = append( batch, line )
			batch_length += sz
		}

		// have we reached a batch size limit
		if len(batch) == batch_size {

			err := sendMessages( cfg, svc, queueUrl, batch )
			if err != nil {
				log.Fatal( err )
			}

			// reset the batch
			batch = batch[:0]
            batch_length = 0
		}

		if count % 100 == 0 {
			duration := time.Since(start)
			log.Printf("Processed %d records (%0.2f tps)", count, float64( count ) / duration.Seconds() )
		}

		if cfg.MaxCount > 0 && count >= cfg.MaxCount  {
			break
		}
	}

	if len(batch) != 0 {

		err := sendMessages( cfg, svc, queueUrl, batch )
		if err != nil {
			log.Fatal( err )
		}
	}

	duration := time.Since(start)
	log.Printf("Done, processed %d records in %0.2f seconds (%0.2f tps)", count, duration.Seconds(), float64( count ) / duration.Seconds() )
}

func sendMessages( cfg * ServiceConfig, svc * sqs.SQS, queueUrl * string, messages []string) error {

	count := len( messages )
	if count == 0 {
		return nil
	}
	batch := make( []*sqs.SendMessageBatchRequestEntry, 0 )
	for ix, m := range messages {
		batch = append( batch, constructMessage( cfg, m, ix ) )
	}

	_, err := svc.SendMessageBatch( &sqs.SendMessageBatchInput{
		Entries:     batch,
		QueueUrl:    queueUrl,
	})

	if err != nil {
		return err
	}

	return nil
}

func constructMessage( cfg * ServiceConfig, message string, index int ) * sqs.SendMessageBatchRequestEntry {

	return &sqs.SendMessageBatchRequestEntry{
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"op": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String("add"),
			},
			"src": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String( cfg.FileName ),
			},
			"type": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String("xml"),
			},
		},
		MessageBody: aws.String(message),
		Id:          aws.String( strconv.Itoa( index )),
	}
}

//
// end of file
//