using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaConsumer
{
    class Program
    {
        const string broker = "elevate.kafka.local:9092";
        const string topic = "test-topic-981723498127349817";
        
        static async Task Main(string[] args)
        {
            await DeleteTestData();
            await SetUpData();
            
            Consume();
            await Task.Delay(TimeSpan.FromSeconds(20));
        }

        static void Consume()
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = broker,
                GroupId = "test-" + Guid.NewGuid(),
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            
            var consumer = new ConsumerBuilder<byte[], byte[]>(consumerConfig).Build();
            
            foreach (var partition in Enumerable.Range(0, 9))
            {
                // just to make sure we actually have data
                var watermark = consumer.QueryWatermarkOffsets(new TopicPartition(topic, partition), TimeSpan.FromSeconds(1));
                Console.WriteLine($"partition {partition}, watermark {watermark}");
            }

            consumer.Assign(Enumerable.Range(0, 9).Select(partition => new TopicPartition(topic, partition)));
            
            // this shows the same behaviour
            //consumer.Assign(Enumerable.Range(0, 9).Select(partition => new TopicPartitionOffset(topic, partition, 0)));
        }

        static async Task SetUpData()
        {
            var adminConfig = new AdminClientConfig
            {
                BootstrapServers = broker,
            };
            var admin = new AdminClientBuilder(adminConfig).Build();
            var topicSpec = new TopicSpecification
            {
                Name = topic,
                NumPartitions = 9,
                ReplicationFactor = 1,
            };
            await admin.CreateTopicsAsync(new[] {topicSpec});
            await Task.Delay(TimeSpan.FromSeconds(1));
            
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = broker,
            };

            Console.WriteLine("Producing test data");
            var rnd = new Random();
            foreach (var i in Enumerable.Range(0, 720))
            {
                var producer = new ProducerBuilder<byte[], byte[]>(producerConfig).Build();
                foreach (var v in Enumerable.Range(0, 200))
                {
                    var value = new byte[20000];
                    rnd.NextBytes(value);
                    var msg = new Message<byte[], byte[]>
                    {
                        Key = Encoding.ASCII.GetBytes(v.ToString()),
                        Value = value,
                    };
                    producer.Produce(topic, msg, sent =>
                    {
                        if (sent.Error.IsError)
                        {
                            Console.WriteLine("ERROR " + sent.Error);
                            throw new Exception(sent.Error.ToString());
                        }
                    });
                }
                Console.WriteLine("Flushing producer");
                producer.Flush();
            }

            Console.WriteLine("Produced test data");
        }
        
        static async Task DeleteTestData()
        {
            var adminConfig = new AdminClientConfig
            {
                BootstrapServers = broker,
            };
            var admin = new AdminClientBuilder(adminConfig).Build();
            var metadata = admin.GetMetadata(TimeSpan.FromSeconds(1));
            
            var testTopics = metadata.Topics
                .Select(t => t.Topic)
                .Where(t => t.StartsWith("test-"))
                .ToList();
            foreach (var t in testTopics)
                Console.WriteLine("Deleting " + t);
            await admin.DeleteTopicsAsync(testTopics);
        }
        
    }
}