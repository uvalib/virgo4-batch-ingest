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

	log.Printf("===> %s service staring up <===", os.Args[ 0 ] )

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs( awssqs.AwsSqsConfig{ } )
	if err != nil {
		log.Fatal( err )
	}

	// get the queue handle from the queue name
	outQueueHandle, err := aws.QueueHandle( cfg.OutQueueName )
	if err != nil {
		log.Fatal( err )
	}

	file, err := os.Open( cfg.FileName )
	if err != nil {
		log.Fatal( err )
	}
	defer file.Close( )

	reader := bufio.NewReader( file )
	block := make( []string, 0, awssqs.MAX_SQS_BLOCK_COUNT )

	count := uint( 0 )
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

		sz := uint( len( line ) )
		if sz >= awssqs.MAX_SQS_MESSAGE_SIZE {
			log.Printf("Ignoring record %d as too large (%d characters)", count, sz )
			continue
		}

		count ++
    	block = append(block, line )

		// have we reached a block size limit
		if count % awssqs.MAX_SQS_BLOCK_COUNT == awssqs.MAX_SQS_BLOCK_COUNT - 1 {

			err := sendMessages( cfg, aws, outQueueHandle, block)
			if err != nil {
				log.Fatal( err )
			}

			// reset the block
			block = block[:0]
		}

		if count % 100 == 0 {
			duration := time.Since(start)
			log.Printf("Processed %d records (%0.2f tps)", count, float64( count ) / duration.Seconds() )
		}

		if cfg.MaxCount > 0 && count >= cfg.MaxCount  {
			break
		}
	}

	// any remaining records?
	if len(block) != 0 {

		err := sendMessages( cfg, aws, outQueueHandle, block)
		if err != nil {
			log.Fatal( err )
		}
	}

	duration := time.Since(start)
	log.Printf("Done, processed %d records in %0.2f seconds (%0.2f tps)", count, duration.Seconds(), float64( count ) / duration.Seconds() )
}

func sendMessages( cfg * ServiceConfig, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, messages []string) error {

	count := len( messages )
	if count == 0 {
		return nil
	}
	batch := make( []awssqs.Message, 0, count )
	for _, m := range messages {
		batch = append( batch, constructMessage( cfg.FileName, m ) )
	}

	opStatus, err := aws.BatchMessagePut( queue, batch )
	if err != nil {
		return err
	}

	// check the operation results
	for ix, op := range opStatus {
		if op == false {
			log.Printf( "WARNING: message %d failed to send to outbound queue", ix )
		}
	}

	return nil
}

func constructMessage( filename string, message string ) awssqs.Message {

	attributes := make( []awssqs.Attribute, 0, 3 )
	attributes = append( attributes, awssqs.Attribute{ "op", "add" } )
	attributes = append( attributes, awssqs.Attribute{ "src", filename } )
	attributes = append( attributes, awssqs.Attribute{ "type", "xml"} )
	return awssqs.Message{ Attribs: attributes, Payload: awssqs.Payload( message )}
}

//
// end of file
//