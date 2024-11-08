// Import required modules
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0'
const v8 = require('v8')
const express = require('express');
const cors = require('cors');
const mqtt = require('mqtt');
const Minio = require('minio');
const multer = require('multer');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const { WebSocket, WebSocketServer } = require("ws");
const http = require("http");
const { PassThrough } = require('stream')

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cors());

const upload = multer({ storage: multer.memoryStorage() });

console.log(v8.getHeapStatistics().heap_size_limit / 1024 / 1024);

//MQTT broker setup
const brokerUrl = "tcp://192.168.60.33:1883";
const mqttClient = mqtt.connect(brokerUrl);

const cipProto = protoLoader.loadSync("bag.proto", {});
const gRPCObject = grpc.loadPackageDefinition(cipProto);
const cips = gRPCObject.bag;

const minioClient = new Minio.Client({
    endPoint: '192.168.60.33',
    port: 9000,
    useSSL: true,
    accessKey: 'minioadmin',
    secretKey: 'minioadmin',
});

const BUCKET_NAME = "tipbucket";

let cipList = [];
const equipmentData = new Map();
let subscribedTopics = new Set();

// Create an HTTP server and a WebSocket server
const cipsSocketServer = http.createServer();
const bagSocketServer = http.createServer();
const cipEquipServer = new WebSocketServer({ noServer: true, maxPayload: 1024 * 64 });
const bagServer = new WebSocketServer({ noServer: true });
const port = 8081;
const otherPort = 8082;

// Start the WebSocket server
cipsSocketServer.listen(8081, () => {
    console.log(`WebSocket server is running on port ${port}`);
});

bagSocketServer.listen(8082, () => {
    console.log(`Websocket server other is running on ${otherPort}`)
});

cipsSocketServer.on('upgrade', function (request, socket, head) {
    if (request.url === "/") {
        cipEquipServer.handleUpgrade(request, socket, head, function (server) {
            cipEquipServer.emit('connection', server, request);
        })
    }
});

bagSocketServer.on('upgrade', function (request, socket, head) {
    if (request.url === "/") {
        bagServer.handleUpgrade(request, socket, head, function (bagWebServer) {
            bagServer.emit('connection', bagWebServer, request);
        })
    }
});

// Handle new client connections
function handleWebSocketConnections(server, label) {
    server.on('connection', (client) => {
        console.log(`New ${label} connection`);
        client.on('close', () => {
            console.log(`${label} WebSocket client disconnected`);
            client = null;
        });
        client.on('error', (err) => console.error(`${label} WebSocket error:`, err));
    });
}

handleWebSocketConnections(cipEquipServer, 'CIP');
handleWebSocketConnections(bagServer, 'Bag');

// MQTT connection
mqttClient.on('connect', () => {
    subscribeToTopic('sendstatus');
    subscribeToTopic('nodes/equipmentlist/#');
});

// Function to subscribe to MQTT topics
function subscribeToTopic(topic) {
    if (!subscribedTopics.has(topic)) {
        mqttClient.subscribe(topic, { qos: 1 }, (err) => {
            if (err) console.error(`Failed to subscribe to ${topic}:`, err.message);
            else subscribedTopics.add(topic);
        });
    }
}

mqttClient.on('message', (topic, message) => {
    try {
        const parsedMessage = JSON.parse(message.toString());
        if (topic === 'sendstatus') {

            handleCipStatusUpdate(parsedMessage);

        } else if (topic.includes('equipmentlist')) {

            equipmentData.set(parsedMessage.nodeID, parsedMessage);
        }
        else if (topic.includes("tipStatusUpdate")) {

            console.log("INSIDE: ", parsedMessage)
            sendBagData("tip", parsedMessage);
        }
        else {
            sendBagData('Bag', parsedMessage);
        }
        message = null;
    } catch (error) {
        console.error('Error processing MQTT message:', error);
    }
});

// Handle CIP status updates
function handleCipStatusUpdate(cipClientInfo) {
    const existingClientIndex = cipList.findIndex(client => client.id === cipClientInfo.id);

    if (existingClientIndex > -1) {
        cipList[existingClientIndex] = { ...cipList[existingClientIndex], ...cipClientInfo, lastUpdated: Date.now() };
    } else {
        cipClientInfo.lastUpdated = Date.now();
        cipClientInfo.client = new cips.Cips(`${cipClientInfo.ipaddress}:${cipClientInfo.port}`, grpc.credentials.createInsecure());
        cipList.push(cipClientInfo);
    }
}

// Log Memory in every 10seconds
setInterval(() => {
    logMemoryUsage();
    console.log("filtered CipList size: ", cipList.length, subscribedTopics.size, cipEquipServer.clients.size)
}, 10000);

app.get('/bags', (req, response) => {

    console.log(req.query.id)
    const cip = cipList.find(client => client.id === req.query.id);
    if (!cip) return response.status(404).send('No CIP found');

    const bagList = [];
    cip.client.GetBagList({})
        .on('data', (bag) => bagList.push(bag))
        .on('end', () => response.send(bagList))
        .on('error', (err) => response.status(500).send(err.message));
});

app.get('/bag', (req, res) => {
    try {
        const { id, name, size, modified } = req.query;
        const cip = cipList.find(client => client.id === id.toString());
        const modifyDate = JSON.parse(modified)
        console.log(id, name, modifyDate, size, cip);
        if (cip) {
            cip?.client?.GetBag({ name: name, size: size, modified: modifyDate }, (err, grpcResponse) => {
                if (err) {
                    console.error(err);
                } else {
                    console.log("BAG RESPONSE:...", grpcResponse)
                    const topic = `nodes/${cip.id}/${grpcResponse.requestId}`;

                    mqttClient.subscribe(topic, { qos: 1 }, (err, granted) => {
                        if (err) {
                            console.error(`Error in subscription of ${topic}`, err);
                        } else {
                            console.log("Sucessfully subscribed the topic ", granted);
                        }
                    });
                    res.send(grpcResponse)
                }
            });
        }

    } catch (error) {
        console.error(error)
    }
});

app.get('/equipment', (req, res) => {
    const { id } = req.query;
    const hasKey = equipmentData.has(id)
    const data = equipmentData.get(id);
    if (data)
        res.status(200).send({ message: data });
    else res.status(500).send("No equipment found on this node currently")
})

// Tip - upload Api
app.post('/tipUpload', upload.single('file'), async (req, res) => {
    try {

        if (!req.file) {
            return res.status(400).send("No file uploaded!");
        }

        const { id } = req.body;
        const topic = `nodes/${id}/tipupdate`;
        const tipStatusTopic = "nodes/" + id + "/tipStatusUpdate";
        console.log(tipStatusTopic);
        const fileName = req.file?.originalname;
        const objectName = `${id}/${fileName}`;

        //Create readable stream
        const fileStream = new PassThrough();
        fileStream.end(req.file.buffer);

        try {
            await minioClient.putObject(BUCKET_NAME, objectName, fileStream, (err, etag) => {
                if (err) {
                    return res.status(500).send(err);
                }

                console.log(etag);

                const fileUrl = `${minioClient.protocol}://${minioClient.host}:${minioClient.port}/${BUCKET_NAME}/${objectName}`;

                const filePath = `${BUCKET_NAME}/${objectName}`;

                let payload = JSON.stringify({ fileUrl, filePath: objectName, BUCKET_NAME, fileName });

                mqttClient.subscribe(tipStatusTopic, { qos: 1 }, (err) => {
                    if (err) console.error(`Error in subscribing ${tipStatusTopic}: `, err)
                    else console.log(`Successfully subscribed to ${tipStatusTopic}`)
                });

                mqttClient.publish(topic, payload, { qos: 1 }, (err) => {
                    if (err) {
                        console.log(`Error publishing to topic ${topic}`);
                        return res.send("Failed to publish file on mqtt");
                    }
                    else {
                        res.send({
                            message: "File uploaded",
                            fileUrl,
                            BUCKET_NAME,
                            mqttTopic: topic,
                            filePath,
                            uploadStatus: "Success",
                            tipUpdateStatus: ''
                        });

                    }
                    payload = null;
                });
            });
            req.file.buffer = null;
            fileStream.destroy(true);

        } catch (err) {
            res.status(400).send(err.message);
        }

    } catch (error) {
        res.status(400).send(error.message);
    }
})

app.listen(3001, () => {
    console.log("Server running on port 3000");
});

// // Broadcast WebSocket message to all clients
function broadcastWebSocketMessage(type, message) {
    try {
        // console.log("Number of clients connected: ", cipEquipServer.clients.size)
        let messageBuffer = Buffer.from(JSON.stringify({ type: type, message }));

        cipEquipServer.clients.forEach(client => {
            if (client.readyState === WebSocket.OPEN) {
                if (messageBuffer) {
                    try {
                        client.send(messageBuffer, (err) => {
                            if (err) {
                                console.log("failed to send data:===", err);
                                client.close()
                            } else {
                                messageBuffer.fill(0);
                            }
                        });
                        if (global.gc) {
                            console.log(global.gc)
                            console.log("Triggering garbage collection...");
                            global.gc();
                        }


                    } catch (error) {
                        console.error("Error in sending cip/equipment data :", error.message);
                    }

                }
            }

        });

        message = null;
    } catch (err) {
        console.log(err)
    }
}

async function sendBagData(type, message) {
    try {

        bagServer.clients.forEach(async client => {
            if (client.readyState === WebSocket.OPEN) {

                if (message && message?.percentageDownload === "100%") {

                    const url = await minioClient.presignedGetObject(message.bucketName, `${message.folderName}/${message.bagName}`, 60 * 60);

                    message = { ...message, url };
                } else {

                    if (message) {
                        message = { ...message, url: null };
                    }
                }
                let messageBuffer = Buffer.from(JSON.stringify({ type: type, message }));
                console.log(message)
                client.send(messageBuffer, (err) => {
                    if (err) {
                        console.log("Failed to send bag start to upload data:", err)
                    } else {
                        console.log(`Successfully sent bag data`);
                    }
                });
                message = null;
                type = null;
                messageBuffer = null;
            }
        });

    } catch (err) {
        console.log(err)
    }

}

process.on("SIGINT", () => {
    console.log("Shutting down....");
    mqttClient.end(true, () => console.log("Disconnected from broker"));
    cipEquipServer.close();
    bagServer.close();

    cipList.forEach(client => {
        if (client.client) {
            grpc.closeClient(client.client);
            delete client.client;
        }
    })

    process.exit(0);
});

setInterval(() => {
    if (global.gc) {
        console.log(global.gc)
        console.log("Triggering garbage collection...");
        global.gc();
        logMemoryUsage("After Garbage Collection....")
    }
}, 20000);

function formatMemoryUsage(data) {
    return `${Math.round(data / 1024 / 1024 * 100) / 100} MB`
}

function logMemoryUsage() {
    const memoryUsage = process.memoryUsage();
    console.log(`Memory Usage - RSS: ${formatMemoryUsage(memoryUsage.rss)}, Heap Total: ${formatMemoryUsage(memoryUsage.heapTotal)}, Heap Used: ${formatMemoryUsage(memoryUsage.heapUsed)}, External: ${formatMemoryUsage(memoryUsage.external)}, Array Buffers: ${formatMemoryUsage(memoryUsage.arrayBuffers)}`);
}

setInterval(() => {
    broadcastWebSocketMessage("CIP", cipList);
    // broadcastWebSocketMessage("Equipment", Array.from(equipmentData))
}, 5000);