using System;
using System.IO;
using System.Threading.Tasks;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.Kafka;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace KafkaCars
{
    public static class CreateCar
    {
        [FunctionName("CreateCar")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            [Kafka(BrokerList = "my-confluent-oss-cp-kafka-headless.default.svc:9092")] IAsyncCollector<KafkaEventData<string, CarRecord>> events)

        {
            CarRecord newCar = JsonConvert.DeserializeObject<CarRecord>(await new StreamReader(req.Body).ReadToEndAsync());
            newCar.CarId = Guid.NewGuid().ToString();

            var config = new ProducerConfig
            {
                BootstrapServers = "my-confluent-oss-cp-kafka-headless:9092"
            };

            var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { SchemaRegistryUrl = "my-confluent-oss-cp-schema-registry.default.svc:8081" });

            using (var p = new ProducerBuilder<string, CarRecord>(config)
                .SetKeySerializer(new AvroSerializer<string>(schemaRegistry).AsSyncOverAsync())
                .SetValueSerializer(new AvroSerializer<CarRecord>(schemaRegistry).AsSyncOverAsync())
                .Build())
            {
                try
                {
                    await events.AddAsync(new KafkaEventData<string, CarRecord>()
                    {
                        Key = Guid.NewGuid().ToString(),
                        Value = newCar,
                        Topic = "cars"
                    });
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Delivery failed: {e.Message }");
                }
            }

            return new OkResult();
        }
    }
}