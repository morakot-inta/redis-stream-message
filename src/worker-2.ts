import { Redis } from 'ioredis';

// set up redis connection
const redis = new Redis({ host: 'localhost', port: 6379 });

//  set the Consumer Group and Stream key
const taskStreamKey = 'task_processing_stream';

//  set the Consumer Group name
const consumerGroupName = 'group-1';

// set read position
const readPosition = '$'; // Change to '0' to read from the beginning

// initialize the worker 
async function initializeWorker() {
    try {
        await redis.xgroup('CREATE', taskStreamKey, consumerGroupName, readPosition, 'MKSTREAM');
        console.log(`Consumer group '${consumerGroupName}' created`);
    } catch (error: any) {
        if (!error.message.includes('BUSYGROUP')) {
            throw error;
        }
        console.log(`Consumer group '${consumerGroupName}' already exists`);
    }
}

// worker start 
async function startWorker() {
    try {
        const data = await redis.xreadgroup(
            'GROUP', consumerGroupName, 'worker_2',
            'COUNT', 1,
            'BLOCK', 5000, // read every 5 seconds 
            'STREAMS', taskStreamKey,
            '>' // read new messages only
        );

        if (data) {
            /**
             * data (Array หรือ null)
            │
            ├─► [0] Stream Entry (Array)
            │   ├─► [0] Stream Name (String): "task_processing_stream"
            │   └─► [1] Messages (Array)
            │       ├─► [0] Message Entry (Array)
            │       │   ├─► [0] Message ID (String): "1737123456789-0"
            │       │   └─► [1] Fields (Array): ["key1", "val1", "key2", "val2", ...]
            │       │
            │       └─► [1] Message Entry (Array)
            │           ├─► [0] Message ID (String): "1737123456789-1"
            │           └─► [1] Fields (Array): ["key1", "val1", "key2", "val2", ...]
            │
            └─► [1] Stream Entry (Array) ← ถ้าอ่านหลาย Stream พร้อมกัน
                └─► ...
             */
            for (const [stream, messages] of data) {
                for (const [id, fields] of messages) {
                    // แปลง fields array เป็น object
                    const taskData: Record<string, string> = {};
                    for (let i = 0; i < fields.length; i += 2) {
                        taskData[fields[i]] = fields[i + 1];
                    }
                    if (taskData.taskBody) {
                        const parsedBody = JSON.parse(taskData.taskBody);
                        console.log("parsedBody:", parsedBody.taskDetails);
                    }

                }
            }

        }
    } catch (error) {
        console.error('Error processing task:', error);
    }
    
    // Continue the loop regardless of whether we received data
    setImmediate(startWorker);
}

// Run the worker
initializeWorker().then(() => {
    startWorker();
});





     
