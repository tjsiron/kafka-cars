using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.WebJobs.Extensions.Kafka;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;

namespace KafkaCars
{
    public static class CarFunctions
    {
        private static readonly ISchemaRegistryClient SchemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = "my-confluent-oss-cp-schema-registry.default.svc:8081" });

        private static readonly AvroDeserializer<CarRecord> Deserializer = new AvroDeserializer<CarRecord>(SchemaRegistry);

        [FunctionName("Cars-Avro")]
        public static async void RunAvro(
           [KafkaTrigger("my-confluent-oss-cp-kafka-headless.default.svc:9092", "cars", ConsumerGroup = "cars-saver")] byte[][] kafkaEvents,
           [Kafka(BrokerList = "my-confluent-oss-cp-kafka-headless.default.svc:9092")] IAsyncCollector<KafkaEventData<string, string>> events)
        {
            foreach (var kafkaEvent in kafkaEvents)
            {
                var car = await Deserializer.DeserializeAsync(kafkaEvent, false, SerializationContext.Empty);
                
                var test = $"Custom deserialised car from batch: {JsonConvert.SerializeObject(car)}";

                await events.AddAsync(new KafkaEventData<string, string>()
                {
                    Value = test,
                    Key = Guid.NewGuid().ToString(),
                    Topic = "cars.reply"
                });
            }
        }
        
        [FunctionName("Cars-Json")]
        public static void RunKafka(
            [KafkaTrigger("my-confluent-oss-cp-kafka-headless.default.svc:9092", "cars.reply", ConsumerGroup = "cars-saver")] KafkaEventData<string, string> kafkaEvent) 
        {
            var test = $"Custom deserialised car from batch: { kafkaEvent.Value }";
            
            Console.WriteLine(test);
        }
    }
}
