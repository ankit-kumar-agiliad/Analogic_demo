// const equipmentInfo = require("./equipment.json");
const mqtt = require('mqtt');

const brokerUrl = "tcp://192.168.60.33:1883"
const mqttClient = mqtt.connect(brokerUrl);

mqttClient.on("connect", () => {
    console.log("Subscribing to topic...");

    mqttClient.subscribe("bootup", (err) => {
        if (err) {
            console.log(`Error in subscribing to bootup`);
        }
        else {
            console.log(`Subscribed to bootup`);
        }
    });
});

mqttClient.on('message', (topic, message) => {
    console.log(topic);

    let cipStatus = JSON.stringify(JSON.parse(message.toString()));
    if (topic === 'bootup') {
        console.log(`Received message from bootup: ${cipStatus}`);
        mqttClient.publish('sendstatus', cipStatus, (err) => {
            if (err) console.error(err);
            else {
                console.log(`Published the CIP status: ${cipStatus}`);
                cipStatus = null;
            }
        })
    }
})

function main(argv) {
    console.log("Starting the cip");
}

main();


process.on("SIGINT", () => {
    console.log("Shutting down....");
    mqttClient.end(true, () => console.log("Disconnected from broker"));

    process.exit(0);
})