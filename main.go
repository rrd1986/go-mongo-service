package main

import (
	"context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Ticket struct {
	CID           int
	CurrentStatus string
	UpdateStatus  string
	OpenNos       int
	CloseNo       int
}

type OpenItem struct {
	CID   int
	Items []string
}

func main() {
	// MongoDB connection settings
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Ping the MongoDB server to check the connection
	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}

	// Get the "tickets" and "openItems" collections from the database
	db := client.Database("your_database_name")
	ticketsCollection := db.Collection("tickets")
	openItemsCollection := db.Collection("openItems")

	// Create a new ticket
	newTicket := Ticket{
		CID:           1,
		CurrentStatus: "Open",
		UpdateStatus:  "In Progress",
		OpenNos:       3,
		CloseNo:       0,
	}

	// Insert the ticket into the "tickets" collection
	_, err = ticketsCollection.InsertOne(context.Background(), newTicket)
	if err != nil {
		log.Fatal(err)
	}

	// Get the ticket with CID = 1 from the "tickets" collection
	filter := bson.M{"CID": 1}
	var ticket Ticket
	err = ticketsCollection.FindOne(context.Background(), filter).Decode(&ticket)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Ticket CID:", ticket.CID)
	fmt.Println("Current Status:", ticket.CurrentStatus)

	// Create a new open item associated with CID = 1
	newOpenItem := OpenItem{
		CID:   1,
		Items: []string{"Item 1", "Item 2"},
	}

	// Get the "openItems" collection from the database
	openItemsCollection = db.Collection("openItems")

	// Insert the open item into the "openItems" collection
	_, err = openItemsCollection.InsertOne(context.Background(), newOpenItem)
	if err != nil {
		log.Fatal(err)
	}

	// Get the open items associated with CID = 1 from the "openItems" collection
	openItemsFilter := bson.M{"CID": 1}
	var openItems OpenItem
	err = openItemsCollection.FindOne(context.Background(), openItemsFilter).Decode(&openItems)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Open Items for CID 1:", openItems.Items)

	// Perform the join operation using the aggregation framework
	lookupStage := bson.D{
		{"$lookup", bson.D{
			{"from", "openItems"},
			{"localField", "CID"},
			{"foreignField", "CID"},
			{"as", "items"},
		}},
	}

	// Match stage to filter the tickets by a specific condition if needed
	matchStage := bson.D{
		{"$match", bson.D{
			{"CurrentStatus", "Open"},
		}},
	}

	// Pipeline combining the match and lookup stages
	pipeline := mongo.Pipeline{matchStage, lookupStage}

	// Execute the aggregation pipeline
	cursor, err := ticketsCollection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.Background())

	// Iterate over the aggregation results
	for cursor.Next(context.Background()) {
		var result struct {
			Ticket  Ticket
			OpenIDs []OpenItem `bson:"items"`
		}
		if err := cursor.Decode(&result); err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Ticket CID: %d\n", result.Ticket.CID)
		fmt.Printf("Open Items:\n")
		for _, item := range result.OpenIDs {
			fmt.Println(item.Items)
		}
	}
	if err := cursor.Err(); err != nil {
		log.Fatal(err)
	}
}
