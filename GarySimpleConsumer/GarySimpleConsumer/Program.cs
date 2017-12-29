using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;

namespace GarySimpleConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Consuming Test Topic");

            var config = new Dictionary<string, object> {
                {"group.id", "" + DateTime.Now + ""},
                {"client.id", "test client id 001"},
                {"enable.auto.commit", false},
                {"session.timeout.ms", 300000},
                {"queued.max.messages.kbytes", 500},
                {"heartbeat.interval.ms", 3000},
                {"auto.offset.reset", "smallest"},
                {"bootstrap.servers", "10.123.11.168:29092,10.123.11.146:29092,10.123.11.158:29092"}
            };

            var orderIdicator1 = 0;
            var orderIdicator2 = 0;
            var orderIdicator3 = 0;
            var orderIdicator4 = 0;
            var orderIdicator0 = 0;
            var partitionIndicator1 = 0;
            var partitionIndicator2 = 0;
            var partitionIndicator3 = 0;
            var partitionIndicator4 = 0;
            var partitionIndicator0 = 0;
            var outOfOrderIndicator = 0;
            var partitionInconsistancy = 0;
            var die = false;
            DateTime timeOfLastMessage = DateTime.Now;
            TimeSpan diff = new TimeSpan();
            var k0p0 = 0;
            var k0p1 = 0;
            var k0p2 = 0;
            var k1p0 = 0;
            var k1p1 = 0;
            var k1p2 = 0;
            var k2p0 = 0;
            var k2p1 = 0;
            var k2p2 = 0;
            var k3p0 = 0;
            var k3p1 = 0;
            var k3p2 = 0;
            var k4p0 = 0;
            var k4p1 = 0;
            var k4p2 = 0;

            using (var consumer = new Consumer<string, string>(config, new StringDeserializer(Encoding.UTF8), new StringDeserializer(Encoding.UTF8)))
            {
                List<string> topicsList = new List<string>();
                topicsList.Add("garytest123");

                Console.WriteLine($"Consumer subscribing to {topicsList[0]}");
                consumer.Subscribe(topicsList);

                consumer.OnError += (sender, error) => throw new Exception(error.Reason);

                

                consumer.OnMessage += (_, message) =>
                {
                    try
                    {

                        if (message.Offset % 1000 == 0)
                        {
                            Console.WriteLine($"Consuming message topicPartitionOffset : {message.TopicPartitionOffset} ");
                            Console.WriteLine($"key : {message.Key} ");
                            Console.WriteLine($"timestamp :  {message.Timestamp} ");
                            Console.WriteLine($"value : {message.Value}");
                        }

                        switch (message.Key)
                        {
                            case "00000000":
                                if (orderIdicator0 > Int32.Parse(message.Value)) outOfOrderIndicator++;
                                orderIdicator0 = Int32.Parse(message.Value);
                                if (partitionIndicator0 != message.Partition) partitionInconsistancy++;
                                partitionIndicator0 = message.Partition;
                                if (message.Partition == 0) k0p0++;
                                if (message.Partition == 1) k0p1++;
                                if (message.Partition == 2) k0p2++;
                                break;
                            case "00000001" :
                                if (orderIdicator1 > Int32.Parse(message.Value)) outOfOrderIndicator++;
                                orderIdicator1 = Int32.Parse(message.Value);
                                if (partitionIndicator1 != message.Partition) partitionInconsistancy++;
                                partitionIndicator1 = message.Partition;
                                if (message.Partition == 0) k1p0++;
                                if (message.Partition == 1) k1p1++;
                                if (message.Partition == 2) k1p2++;
                                break;
                            case "00000002":
                                if (orderIdicator2 > Int32.Parse(message.Value)) outOfOrderIndicator++;
                                orderIdicator2 = Int32.Parse(message.Value);
                                if (partitionIndicator2 != message.Partition) partitionInconsistancy++;
                                partitionIndicator2 = message.Partition;
                                if (message.Partition == 0) k2p0++;
                                if (message.Partition == 1) k2p1++;
                                if (message.Partition == 2) k2p2++;
                                break;
                            case "00000003":
                                if (orderIdicator3 > Int32.Parse(message.Value)) outOfOrderIndicator++;
                                orderIdicator3 = Int32.Parse(message.Value);
                                if (partitionIndicator3 != message.Partition) partitionInconsistancy++;
                                partitionIndicator3 = message.Partition;
                                if (message.Partition == 0) k3p0++;
                                if (message.Partition == 1) k3p1++;
                                if (message.Partition == 2) k3p2++;
                                break;
                            case "00000004":
                                if (orderIdicator4 > Int32.Parse(message.Value)) outOfOrderIndicator++;
                                orderIdicator4 = Int32.Parse(message.Value);
                                if (partitionIndicator4 != message.Partition) partitionInconsistancy++;
                                partitionIndicator4 = message.Partition;
                                if (message.Partition == 0) k4p0++;
                                if (message.Partition == 1) k4p1++;
                                if (message.Partition == 2) k4p2++;
                                break;
                        }

                        if (message.Offset % 5 != 0)
                        {
                            consumer.CommitAsync(message);
                            //Console.WriteLine($"Asynchronously Commiting message of offset {message.Offset}");
                        }
                        if (message.Offset % 5 == 0)
                        {
                            //Console.WriteLine($"Synchronously Commiting async message of offset {message.Offset}, then getting awaiter");
                            consumer.CommitAsync(message).GetAwaiter();
                        }
                        timeOfLastMessage = DateTime.Now;
                    }
                    catch (Exception exception)
                    {
                        throw exception;
                    }
                };
                
                while (!die)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(100));
                    diff = DateTime.Now - timeOfLastMessage;
                    if (diff.TotalMilliseconds > 15000)
                    {
                        die = true;
                    }
                }
                Console.WriteLine($"k0p0={k0p0} k0p1={k0p1} k0p2={k0p2}");
                Console.WriteLine($"k1p0={k1p0} k1p1={k1p1} k1p2={k1p2}");
                Console.WriteLine($"k2p0={k2p0} k2p1={k2p1} k2p2={k2p2}");
                Console.WriteLine($"k3p0={k3p0} k3p1={k3p1} k3p2={k3p2}");
                Console.WriteLine($"k4p0={k4p0} k4p1={k4p1} k4p2={k4p2}");
                Console.WriteLine($"partitionInconsistancy : {partitionInconsistancy - 3} outOfOrderIndicator : {outOfOrderIndicator}");
                consumer.Dispose();
            }
        }
    }
}
