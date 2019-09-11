package main

import (
	"bufio"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

//
// main entry point
//
func main() {

	//log.Printf("===> V4 batch ingest service staring up <===")

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

	scanner := bufio.NewScanner( file )
	for scanner.Scan( ) {

		result, err := svc.SendMessage( &sqs.SendMessageInput{
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
					StringValue: aws.String( "text" ),
				},
			},
			MessageBody: aws.String( scanner.Text() ),
			QueueUrl:    result.QueueUrl,
		})

		if err != nil {
			log.Fatal( err )
		}

		log.Printf("Success: %s", *result.MessageId)
	}

	if err := scanner.Err( ); err != nil {
		log.Fatal(err)
	}
}
