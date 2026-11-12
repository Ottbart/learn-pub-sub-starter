package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connect := "amqp://guest:guest@localhost:5672/"
	amqpConnection, err := amqp.Dial(connect)
	if err != nil {
		fmt.Printf("error connecting to amqp at URL: %v\nError: %v", connect, err)
	}
	defer amqpConnection.Close()
	fmt.Println("successfully connected to amqp")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("error in ClientWelcome: %v", err)
	}

	/*
		// Declare and bind a queue for this client to receive pause messages
		ch, queue, err := pubsub.DeclareAndBind(amqpConnection, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, "transient")
		if err != nil {
			log.Fatalf("could not subscribe to pause queue: %v", err)
		}
		fmt.Printf("Queue %v declared and bound!\nChanel: %v", queue.Name, ch)
	*/
	gameState := gamelogic.NewGameState(username)
	publishChannel, err := amqpConnection.Channel()
	if err != nil {
		fmt.Printf("error creating publish channel: %v\n", err)
		return
	}
	defer publishChannel.Close()

	//subscribe to pause game handler
	if err := pubsub.SubscribeJSON(
		amqpConnection,
		routing.ExchangePerilDirect,
		"pause."+username,
		routing.PauseKey,
		pubsub.TransientQueue,
		handlerPause(gameState),
	); err != nil {
		fmt.Printf("could not subscribe to pause queue: %v\n", err)
	}
	fmt.Println("subscribed to pause game handler")

	// subscribe to player moves handler
	if err := pubsub.SubscribeJSON(
		amqpConnection,
		routing.ExchangePerilTopic,
		"army_moves."+username,
		routing.ArmyMovesPrefix+".*",
		pubsub.TransientQueue,
		handlerMove(gameState, publishChannel),
	); err != nil {
		fmt.Printf("could not subscribe to player moves queue: %v\n", err)
	}
	fmt.Println("subscribed to player moves game handler")

	//subscribe to war handler
	if err := pubsub.SubscribeJSON(
		amqpConnection,
		routing.ExchangePerilTopic,
		"war",
		routing.WarRecognitionsPrefix+".*",
		pubsub.DurableQueue,
		handlerConsumeWarMessages(gameState, publishChannel),
	); err != nil {
		fmt.Printf("could not subscribe to war queue: %v\n", err)
	}
	fmt.Println("subscribed to war handler")

	//Start REPL
	for {
		cmd := gamelogic.GetInput()
		if len(cmd) == 0 {
			continue
		}
		switch cmd[0] {
		case "spawn":
			err := gameState.CommandSpawn(cmd)
			if err != nil {
				fmt.Printf("error in gamestate.CommandSpawn: %v\n", err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(cmd)
			if err != nil {
				fmt.Printf("error in gamestate.CommandMove: %v\n", err)
				continue
			}
			key := routing.ArmyMovesPrefix + "." + username
			if err := pubsub.PublishJSON(
				publishChannel,
				routing.ExchangePerilTopic,
				key,
				move,
			); err != nil {
				fmt.Printf("error publishing move: %v\n", err)
				continue
			}
			fmt.Println("Move published successfully")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			multi, err := strconv.Atoi(cmd[1])
			if err != nil {
				fmt.Printf("invalid number %q: %v\n", cmd[1], err)
				continue
			}
			for i := 0; i < multi; i++ {
				logMessage := gamelogic.GetMaliciousLog()
				pubsub.PublishJSON(
					publishChannel,
					routing.ExchangePerilTopic,
					routing.GameLogSlug+"."+username,
					logMessage,
				)
			}
			fmt.Printf("Published %v malicious logs\n", multi)
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("error, unknown command")
		}
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(move)
		switch moveOutcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			if err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			); err != nil {
				fmt.Printf("error publishing move: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		fmt.Println("error: unknown move outcome")
		return pubsub.NackDiscard
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerConsumeWarMessages(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		warOutcome, winner, loser := gs.HandleWar(rw)
		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			logString := fmt.Sprintf("%v won a war against %v", winner, loser)
			if err := publishGameLog(ch, gs.Player.Username, logString); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			logString := fmt.Sprintf("%v won a war against %v", winner, loser)
			if err := publishGameLog(ch, gs.Player.Username, logString); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			logString := fmt.Sprintf("A war between %v and %v resulted in a draw", winner, loser)
			if err := publishGameLog(ch, gs.Player.Username, logString); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("error handling war outcome")
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(ch *amqp.Channel, username, message string) error {
	if err := pubsub.PublishGob(
		ch,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+username,
		routing.GameLog{
			CurrentTime: time.Now(),
			Message:     message,
			Username:    username,
		},
	); err != nil {
		return fmt.Errorf("error publishing move: %v\n", err)
	}
	return nil
}
