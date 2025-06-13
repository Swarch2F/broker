const express = require('express');
const amqp = require('amqplib');
const axios = require('axios');
const cors = require('cors');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://rabbitmq:5672';

// Middleware
app.use(cors());
app.use(express.json());

// RabbitMQ configuration
let channel;
const QUEUES = {
    HIGH_PRIORITY: 'assignments_high_priority',
    NORMAL_PRIORITY: 'assignments_normal_priority',
    LOW_PRIORITY: 'assignments_low_priority',
    ERROR_QUEUE: 'assignments_error'
};
const MAX_RETRIES = 30;
const RETRY_INTERVAL = 5000;
const MAX_RETRY_ATTEMPTS = 3;

// Message processing status
const messageStatus = new Map();

async function connectToRabbitMQ(retryCount = 0) {
    try {
        console.log(`Attempting to connect to RabbitMQ at ${RABBITMQ_URL} (attempt ${retryCount + 1}/${MAX_RETRIES})`);
        const connection = await amqp.connect(RABBITMQ_URL);
        channel = await connection.createChannel();
        
        // Assert all queues
        for (const queue of Object.values(QUEUES)) {
            await channel.assertQueue(queue, {
                durable: true,
                arguments: {
                    'x-message-ttl': 86400000, // 24 hours
                    'x-dead-letter-exchange': '',
                    'x-dead-letter-routing-key': QUEUES.ERROR_QUEUE
                }
            });
        }
        
        console.log('Successfully connected to RabbitMQ and initialized queues');
        return true;
    } catch (error) {
        console.error('Error connecting to RabbitMQ:', error.message);
        if (retryCount < MAX_RETRIES) {
            console.log(`Retrying in ${RETRY_INTERVAL/1000} seconds...`);
            await new Promise(resolve => setTimeout(resolve, RETRY_INTERVAL));
            return connectToRabbitMQ(retryCount + 1);
        }
        throw error;
    }
}

function determinePriority(path, method) {
    // Lógica para determinar la prioridad basada en el tipo de operación
    if (method === 'GET') return QUEUES.HIGH_PRIORITY;
    if (method === 'POST' || method === 'PUT') return QUEUES.NORMAL_PRIORITY;
    return QUEUES.LOW_PRIORITY;
}

async function processMessage(message, retryCount = 0) {
    try {
        const response = await axios({
            method: message.method,
            url: `http://gx_be_proasig:8080${message.path}`,
            data: message.body,
            headers: message.headers
        });

        return {
            success: true,
            data: response.data
        };
    } catch (error) {
        if (retryCount < MAX_RETRY_ATTEMPTS) {
            console.log(`Retrying message processing (attempt ${retryCount + 1}/${MAX_RETRY_ATTEMPTS})`);
            await new Promise(resolve => setTimeout(resolve, 1000 * (retryCount + 1)));
            return processMessage(message, retryCount + 1);
        }
        
        return {
            success: false,
            error: error.message
        };
    }
}

async function consumeMessages() {
    if (!channel) {
        throw new Error('Channel not initialized');
    }

    // Consume from all priority queues
    for (const queue of Object.values(QUEUES)) {
        if (queue === QUEUES.ERROR_QUEUE) continue;

        channel.consume(queue, async (data) => {
            if (!data) return;

            const message = JSON.parse(data.content);
            const messageId = message.id || Math.random().toString();
            console.log(`Processing message ${messageId} from queue ${queue}`);

            try {
                const result = await processMessage(message);
                
                if (result.success) {
                    // Send response back through RabbitMQ
                    if (data.properties.replyTo) {
                        channel.sendToQueue(
                            data.properties.replyTo,
                            Buffer.from(JSON.stringify(result.data)),
                            {
                                correlationId: data.properties.correlationId
                            }
                        );
                    }
                    channel.ack(data);
                    messageStatus.set(messageId, 'completed');
                } else {
                    // Move to error queue
                    channel.sendToQueue(
                        QUEUES.ERROR_QUEUE,
                        Buffer.from(JSON.stringify({
                            ...message,
                            error: result.error,
                            originalQueue: queue
                        }))
                    );
                    channel.ack(data);
                    messageStatus.set(messageId, 'error');
                }
            } catch (error) {
                console.error(`Error processing message ${messageId}:`, error);
                channel.nack(data);
                messageStatus.set(messageId, 'failed');
            }
        });
    }

    // Handle error queue
    channel.consume(QUEUES.ERROR_QUEUE, async (data) => {
        if (!data) return;
        
        const message = JSON.parse(data.content);
        console.log('Processing error message:', message);
        
        // Log error and acknowledge
        channel.ack(data);
    });

    console.log('All consumers started successfully');
}

// API endpoint to receive requests
app.all('/broker/*', async (req, res) => {
    if (!channel) {
        return res.status(503).json({ error: 'Service temporarily unavailable' });
    }

    try {
        const messageId = Math.random().toString();
        const correlationId = Math.random().toString();
        const replyQueue = await channel.assertQueue('', { exclusive: true });

        // Prepare message
        const message = {
            id: messageId,
            method: req.method,
            path: req.path.replace('/broker', ''),
            body: req.body,
            headers: req.headers,
            timestamp: new Date().toISOString()
        };

        // Determine priority queue
        const priorityQueue = determinePriority(message.path, message.method);

        // Send message to appropriate queue
        channel.sendToQueue(priorityQueue, Buffer.from(JSON.stringify(message)), {
            correlationId,
            replyTo: replyQueue.queue,
            messageId
        });

        messageStatus.set(messageId, 'queued');

        // Wait for response
        channel.consume(replyQueue.queue, (data) => {
            if (data && data.properties.correlationId === correlationId) {
                const response = JSON.parse(data.content);
                res.json(response);
                channel.deleteQueue(replyQueue.queue);
                messageStatus.set(messageId, 'responded');
            }
        }, { noAck: true });

    } catch (error) {
        console.error('Error processing request:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ 
        status: channel ? 'healthy' : 'unhealthy',
        queues: Object.keys(QUEUES),
        messageStatus: Object.fromEntries(messageStatus)
    });
});

// Start server
async function startServer() {
    try {
        await connectToRabbitMQ();
        await consumeMessages();
        
        app.listen(PORT, () => {
            console.log(`Broker server running on port ${PORT}`);
        });
    } catch (error) {
        console.error('Failed to start server:', error);
        process.exit(1);
    }
}

startServer(); 