package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

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

	file, err := os.Open( cfg.FileName )
	if err != nil {
		log.Fatal( err )
	}
	defer file.Close( )

	reader := bufio.NewReader( file )

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
		if sz >= 262144 {
			log.Printf("Ignoring record %d as too large (%d characters)", count, sz )
			continue
		}

		_, err = svc.SendMessage( &sqs.SendMessageInput{
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
					StringValue: aws.String( "xml" ),
				},
			},
			MessageBody: aws.String( string( line ) ),
			QueueUrl:    result.QueueUrl,
		})

		if err != nil {
			log.Fatal( err )
		}

		count ++
		duration := time.Since(start)
		if count % 100 == 0 {
			log.Printf("Processed %d records (%0.2f tps)", count, float64( count ) / duration.Seconds() )
		}
	}
	duration := time.Since(start)
	log.Printf("Done, processed %d records (%0.2f tps)", count, float64( count ) / duration.Seconds() )
}
