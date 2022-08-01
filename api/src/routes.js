import express from 'express';
import { CompressionTypes } from 'kafkajs';

const routes = express.Router();

routes.post('/publish', async (req, res) => {
  const message = {
    user: { id: 1, name: 'Rodrigo' },
    course: 'Kafka com Node.js',
    grade: 10,
  };

  // Chamar micro serviço
  await req.producer.send({
    topic: 'test-publish',
    compression: CompressionTypes.GZIP,
    messages: [
      { value: JSON.stringify(message) },
      { value: JSON.stringify({ ...message, user: { ...message.user, name: 'Marcelo' } }) },
    ],
  })

  return res.json({ ok: true });
});

export default routes;