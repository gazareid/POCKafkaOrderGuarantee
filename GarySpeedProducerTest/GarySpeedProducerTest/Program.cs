using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;

namespace GarySpeedProducerTest
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Sending Test Topic.");

            var kafkaConfig = new Dictionary<string, object>()
            {
                ["bootstrap.servers"] = "10.123.11.168:29092,10.123.11.146:29092,10.123.11.158:29092",
                ["client.id"] = "akks-arch-demo",
                ["message.timeout.ms"] = 60000,
                ["retries"] = 0,
                ["batch.num.messages"] = 1,
                ["socket.blocking.max.ms"] = 1,
                ["socket.nagle.disable"] = true,
                ["queue.buffering.max.ms"] = 0,
                ["default.topic.config"] = new Dictionary<string, object>()
                {
                    ["acks"] = -1,
                }
            };

            using (var producer = new Producer<string, string>(kafkaConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                producer.OnError += (obj, error) =>
                {
                    Console.WriteLine($"On Error Report: HasError={error.HasError} IsBrokerError={error.IsBrokerError} IsLocalError={error.IsLocalError}");
                    Console.WriteLine($"Reason for Error: {error.Reason}");
                    Console.WriteLine($"Code for Error: {error.Code}");
                };

                var errored = 0;
                var successful = 0;
                var startTime = DateTime.Now;

                for (int index = 0; index < 2000000; index++)
                {
                    var key = "0000000" + index % 5;
                    var msgToSend = new SimpleClass(key, index, 10);
                    //ProduceMessages(key, msgToSend.Msgs.ToString());
                    ProduceMessages(key, index.ToString());
                }

                void ProduceMessages(string key, string msgToSend)
                {
                    var messageResult = producer.ProduceAsync("garytest123", key, msgToSend)
                        .Result;

                    if (Int32.Parse(messageResult.Value) % 100 == 0)
                    {
                        Console.Write($"{Math.Floor(Int32.Parse(messageResult.Value) / Math.Floor((DateTime.Now - startTime).TotalMilliseconds / 1000)) } ");
                        Console.WriteLine($"{messageResult.Value} | {messageResult.TopicPartitionOffset} | {messageResult.Key}");
                    }

                    //do something with the result

                    //.ContinueWith(task =>
                    //{
                    //    if (task.Result.Error.HasError)
                    //    {
                    //        Console.WriteLine($"Resending Message: key={task.Result.Key} offset={task.Result.Offset}");
                    //        ProduceMessages(key, msgToSend);
                    //        errored++;
                    //    }
                    //    if (!task.Result.Error.HasError)
                    //    {
                    //        successful++;
                    //    }
                    //});
                }
                var returned = producer.Flush(70000);

                while (returned > 0)
                {
                    returned = producer.Flush(70000);
                    Console.WriteLine("Flushing ret=" + returned);
                }

                Console.WriteLine("Sent test topic.");
                Console.WriteLine($"errored: {errored} successful: {successful}");

                System.Threading.Thread.Sleep(5000000);
            }
        }






        //Builds a message of guids to simulate a simple message for kafka
        public class SimpleClass
        {
            public string Key { get; set; }
            public int Counter { get; set; }
            public Dictionary<string, string> Msgs { get; set; }
            public int[] IntArray { get; set; }

            public SimpleClass(string key, int counter, int numMsgs)
            {
                Key = key;
                Counter = counter;
                Msgs = new Dictionary<string, string>(10);
                for (int i = 0; i < numMsgs; i++)
                {
                    var textGUID = Guid.NewGuid().ToString();
                    Msgs.Add(textGUID, "GUID = " + textGUID);
                }

                Random rnd = new Random();
                var size = rnd.Next(1, 50);
                IntArray = new int[size];
                for (int j = 0; j < size; j++)
                {
                    IntArray[j] = rnd.Next();
                }
            }
        }
    }
}