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
        private static ISchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = "my-confluent-oss-cp-schema-registry.default.svc:8081" });

        private static AvroDeserializer<CarRecord> deserializer = new AvroDeserializer<CarRecord>(schemaRegistry);

        [FunctionName("Cars")]
        public static async void Run(
           [KafkaTrigger("my-confluent-oss-cp-kafka-headless.default.svc:9092", "cars", ConsumerGroup = "cars-saver")] byte[][] kafkaEvents)
        {
            foreach (var kafkaEvent in kafkaEvents)
            {
                var car = await deserializer.DeserializeAsync(kafkaEvent, false, SerializationContext.Empty);
                Console.WriteLine($"Custom deserialised user from batch: {JsonConvert.SerializeObject(car)}");
            }
        }
    }
}
