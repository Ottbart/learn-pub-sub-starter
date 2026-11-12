package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connect := "amqp://guest:guest@localhost:5672/"
	amqpConnection, err := amqp.Dial(connect)
	if err != nil {
		fmt.Printf("error connecting to amqp at URL: %v\nError: %v", connect, err)
	}
	defer amqpConnection.Close()
	fmt.Println("successfully connected to amqp")

	//create new channel
	ch, err := amqpConnection.Channel()
	if err != nil {
		fmt.Printf("error creating channel: %v", err)
	}

	err = pubsub.SubscribeGob(
		amqpConnection,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.DurableQueue,
		func(gameLog routing.GameLog) pubsub.Acktype {
			defer fmt.Print("> ")
			if err := gamelogic.WriteLog(gameLog); err != nil {
				log.Printf("could not write game log: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		})
	if err != nil {
		log.Fatalf("could not subscribe to game_logs: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "pause":
			fmt.Println("sending pause message")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
		case "resume":
			fmt.Println("sending resume message")
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
		case "quit":
			fmt.Println("exiting")
			return
		default:
			fmt.Println("command unknown")
		}

	}
	// wait for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	defer signal.Stop(signalChan)
	<-signalChan
	fmt.Println("shutting down")
}
