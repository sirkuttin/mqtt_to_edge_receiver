package main

import (
	"github.com/sirkuttin/mqtt"
	"github.com/sirkuttin/edge_vehicle_data"
	"fmt"
	"encoding/binary"
	"bytes"
	"os"
	"os/signal"
)


func main() {

	mqttClient, err := mqtt.New("tcp://127.0.0.1:1883", "edge")
	defer mqttClient.Disconnect(2000)
	if err != nil {
		panic(err)
	}

	go func() {
		handle := handleVehiclAlertMessage(mqttClient)
		for ; ; {
			handle()
		}
	}()

	go func() {
		handle := handleWeatherMessage(mqttClient)
		for ; ; {
			handle()
		}
	}()

	close := make(chan os.Signal, 1)
	signal.Notify(close, os.Interrupt)
	<-close
	fmt.Println("Program Exited")
}

func handleVehiclAlertMessage(mqttClient mqtt.Mqtt) (func()){
	vehicleAlertChan := make(chan mqtt.Message)

	err := mqttClient.SubscribeToTopic("vehicle-alert", func(msg mqtt.Message) {
		vehicleAlertChan <- msg
	});

	if err != nil {
		panic(err.Error())
	}

	return func(){
		vehicleAlert := parseVehicleData((<- vehicleAlertChan).Payload)
		fmt.Println("alert_id =", vehicleAlert.AlertId, "| vehicle_id =", vehicleAlert.VehicleId)
	}
}

func parseVehicleData(alertBytes []byte) (newAlert data.Alert) {

	buf := bytes.NewReader(alertBytes)

	if err := binary.Read(buf, binary.LittleEndian, &newAlert); err != nil {
		fmt.Println("binary.Read failed:", err)
	}
	return
}

func handleWeatherMessage(mqttClient mqtt.Mqtt) (func()){
	weatherChan := make(chan mqtt.Message)

	err := mqttClient.SubscribeToTopic("weather", func(msg mqtt.Message) {
		weatherChan <- msg
	});

	if err != nil {
		panic(err.Error())
	}

	return func(){
		weatherData := parseWeatherData((<-weatherChan).Payload)
		fmt.Println(weatherData)
	}
}
func parseWeatherData(weatherBytes []byte) (newWeather data.Weather) {

	buf := bytes.NewReader(weatherBytes)

	if err := binary.Read(buf, binary.LittleEndian, &newWeather); err != nil {
		fmt.Println("binary.Read failed:", err)
	}
	return
}
