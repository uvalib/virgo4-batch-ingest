package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"time"

	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

//
// main entry point
//
func main() {

	log.Printf("===> %s service staring up (version: %s) <===", os.Args[ 0 ], Version( ) )

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs( awssqs.AwsSqsConfig{ MessageBucketName: cfg.MessageBucketName } )
	fatalIfError( err )

	// get the queue handle from the queue name
	outQueueHandle, err := aws.QueueHandle( cfg.OutQueueName )
	fatalIfError( err )

	// create the record channel
	outboundMessageChan := make( chan awssqs.Message, cfg.WorkerQueueSize )

	// start workers here
	for w := 1; w <= cfg.Workers; w++ {
		go worker( w, cfg, aws, outboundMessageChan, outQueueHandle )
	}

	file, err := os.Open( cfg.FileName )
	fatalIfError( err )
	defer file.Close( )

	reader := bufio.NewReader( file )

	count := uint( 0 )
	start := time.Now()

	for {

		line, err := reader.ReadString( '\n' )

		if err != nil {
			// are we done
			if err == io.EOF {
				break
			} else {
				fatalIfError( err )
			}
		}

		//sz := uint( len( line ) )
		//if sz >= awssqs.MAX_SQS_MESSAGE_SIZE {
		//	log.Printf("Ignoring record %d as too large (%d characters)", count, sz )
		//	continue
		//}

		count ++
		outboundMessageChan <- constructMessage( cfg.DataSourceName, awssqs.Payload( line ) )

		if count % 1000 == 0 {
			duration := time.Since(start)
			log.Printf("Processed %d records (%0.2f tps)", count, float64( count ) / duration.Seconds() )
		}

		if cfg.MaxCount > 0 && count >= cfg.MaxCount  {
			log.Printf("Terminating after %d messages", count )
			break
		}
	}

	duration := time.Since(start)
	log.Printf("Done, processed %d records in %0.2f seconds (%0.2f tps)", count, duration.Seconds(), float64( count ) / duration.Seconds() )

	for {
		if len( outboundMessageChan ) == 0 {
			time.Sleep( 10 * time.Second)
			break
		}
		log.Printf("Waiting for workers to complete... zzzz" )
		time.Sleep( 1 * time.Second)
	}
}

func constructMessage( datasource string, message awssqs.Payload ) awssqs.Message {

	attributes := make( []awssqs.Attribute, 0, 2 )
	//attributes = append( attributes, awssqs.Attribute{ "op", "add" } )
	//attributes = append( attributes, awssqs.Attribute{ "src", filename } )
	attributes = append( attributes, awssqs.Attribute{ "type", "xml"} )
	attributes = append( attributes, awssqs.Attribute{ Name: "source", Value: datasource } )
	return awssqs.Message{ Attribs: attributes, Payload: message }
}

//
// end of file
//