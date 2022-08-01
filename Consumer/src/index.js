import { Kafka, CompressionTypes, logLevel } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'consumer',
  brokers: ['localhost:9092'],
  logLevel: logLevel.WARN,
})

const topic = 'test-publish'
const consumer = kafka.consumer({ groupId: 'certificate-group' })

const { Partitioners } = require('kafkajs')
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })

async function run() {
  await consumer.connect()
  await consumer.subscribe({ topic })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} key=${message.key} #${message.value}`)

      const payload = JSON.parse(message.value);

      setTimeout(() => { 
        producer.send({
          topic: 'certification-response',
          compression: CompressionTypes.GZIP,
          messages: [
            { value: `Certificado do usuário ${payload.user.name} do curso ${payload.course} gerado!` }
          ]
        })
      }, 3000);
    },
  })
}

run().catch(console.error)
