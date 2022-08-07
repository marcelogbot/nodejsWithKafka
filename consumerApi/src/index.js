import { Kafka, CompressionTypes, logLevel } from 'kafkajs';

/**
 * Faz conexão com o Kafka
 */
const kafka = new Kafka({
  clientId: 'consumerApi',
  brokers: ['localhost:9092'],
  logLevel: logLevel.WARN,
});

const { Partitioners } = require('kafkajs')

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })
const consumer = kafka.consumer({ groupId: 'certificate-group' })

async function run() {

  await producer.connect()
  await consumer.connect()

  await consumer.subscribe({ topic: 'test-publish' })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic} [${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} key=${message.key} #${message.value}`)

      const payload = JSON.parse(message.value);

      //setTimeout(() => { 
        producer.send({
          topic: 'certification-response',
          compression: CompressionTypes.GZIP,
          messages: [
            { value: `Dados do usuário [${payload.user.name}] do curso [${payload.course}] gerado!` }
          ]
        })
     //}, 3000);
    },
  })
}

run().catch(console.error)
