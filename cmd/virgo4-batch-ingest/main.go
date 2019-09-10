package main

import (
	"bufio"
	"fmt"
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

	file, err := os.Open( cfg.FileName )
	if err != nil {
		log.Fatal( err )
	}
	defer file.Close( )

	scanner := bufio.NewScanner( file )
	for scanner.Scan( ) {

		result, err := svc.SendMessage( &sqs.SendMessageInput{
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				"OP": &sqs.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String("ADD"),
				},
			},
			MessageBody: aws.String( scanner.Text() ),
			QueueUrl:    &cfg.QueueUrl,
		})

		if err != nil {
			log.Fatal( err )
		}

		fmt.Println("Success", *result.MessageId)
//		fmt.Println( scanner.Text() )
	}

	if err := scanner.Err( ); err != nil {
		log.Fatal(err)
	}
}
