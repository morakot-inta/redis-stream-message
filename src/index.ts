import fastify from 'fastify';
import type { FastifyInstance } from 'fastify';
import { Redis } from 'ioredis';

const app: FastifyInstance = fastify({ logger: true });
const redis = new Redis({ host: 'localhost', port: 6379 });

// config stream key
const taskStreamKey = 'task_processing_stream';

app.get('/health', async () => {
  return { status: 'ok' };
});

// applicaton todolist to simulate task processing
interface TaskBody {
  taskName: string;
  taskDetails: string;
}

app.post('/stream', async (request, reply) => {
  const body = request.body as TaskBody;
  if (!body.taskName || !body.taskDetails) {
    reply.status(400).send({ error: 'Invalid task data' });
    return;
  }

  try {
    const streamId = await redis.xadd(
      taskStreamKey,
      '*',
      'taskName', body.taskName,
      'taskBody', JSON.stringify(body)
    );
    reply.status(201).send({ streamId });
  } catch (error) {
    request.log.error(error);
    reply.status(500).send({ error: 'Failed to add task to stream' });
  }
});

app.listen({ port: 3000 }, (err, address) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }
  console.log(`Server listening at ${address}`);
});