package main

import (
	"github.com/sirkuttin/mqtt"
	"github.com/sirkuttin/edge_vehicle_data"
	"encoding/binary"
	"bytes"
	"os"
	"os/signal"
	log "github.com/Sirupsen/logrus"
)

func init() {
	log.SetLevel(log.DebugLevel)
}


func main() {

	mqttClient, err := mqtt.New("tcp://127.0.0.1:1884", "edge")
	defer mqttClient.Disconnect(2000)
	if err != nil {
		log.Fatal(err)
		panic(err)
	}

	log.Info("mqtt client connected")

	go func() {
		handle := handleVehiclAlertMessage(mqttClient)
		log.Info("Starting Vehicle Alert routine")
		for ; ; {
			handle()
		}
	}()

	go func() {
		handle := handleWeatherMessage(mqttClient)
		log.Info("Starting Weather Update routine")
		for ; ; {
			handle()
		}
	}()

	close := make(chan os.Signal, 1)
	signal.Notify(close, os.Interrupt)
	<-close
	log.Info("Program Exited")
}

func handleVehiclAlertMessage(mqttClient mqtt.Client) (func()){
	vehicleAlertChan := make(chan mqtt.Message)

	err := mqttClient.SubscribeToTopic("vehicle-alert", func(msg mqtt.Message) {
		vehicleAlertChan <- msg
	});

	if err != nil {
		panic(err.Error())
	}

	return func(){
		vehicleAlert := parseVehicleData((<- vehicleAlertChan).Payload)
		log.Debug("alert_id =", vehicleAlert.AlertId, "| vehicle_id =", vehicleAlert.VehicleId)
	}
}

func parseVehicleData(alertBytes []byte) (newAlert data.Alert) {

	buf := bytes.NewReader(alertBytes)

	if err := binary.Read(buf, binary.LittleEndian, &newAlert); err != nil {
		log.Error("binary.Read failed:", err)
	}
	return
}

func handleWeatherMessage(mqttClient mqtt.Client) (func()){
	weatherChan := make(chan mqtt.Message)

	err := mqttClient.SubscribeToTopic("weather", func(msg mqtt.Message) {
		weatherChan <- msg
	});

	if err != nil {
		panic(err.Error())
	}

	return func(){
		weatherData := parseWeatherData((<-weatherChan).Payload)
		log.Debug(weatherData)
	}
}
func parseWeatherData(weatherBytes []byte) (newWeather data.Weather) {

	buf := bytes.NewReader(weatherBytes)

	if err := binary.Read(buf, binary.LittleEndian, &newWeather); err != nil {
		log.Error("binary.Read failed:", err)
	}
	return
}
