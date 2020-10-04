using System;
using Microsoft.Azure.EventHubs;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Text;

namespace ConsoleApp18
{
    class Program
    {
        private static EventHubClient eventHubClient;

        private const string EventHubConnectionString = "Endpoint=sb://eh-ai100or-01.servicebus.windows.net/;" +
            "SharedAccessKeyName=RootManageSharedAccessKey;" +
            "SharedAccessKey=De1HAOz94ECVB+zYzQNKAWK4GW4XrMeRdBtqx1Wq2D0=";

        private const string EventHubName = "hub01";

        // list of sent messages
        private static List<TempratureInfo> messages = new List<TempratureInfo>();

        static void Main(string[] args)
        {
            var connectionStringBuilder = new EventHubsConnectionStringBuilder(
                EventHubConnectionString)
            {
                EntityPath = EventHubName
            };

            eventHubClient = EventHubClient.CreateFromConnectionString(
                connectionStringBuilder.ToString());

            // sends events for 1 minute
            SendMessagesToEventHub(1000).Wait();

            eventHubClient.Close();
            
            Console.WriteLine("Press any key to continue ...");
            Console.ReadKey();
        }

        // Sends {n} messages to event hub.
        private static async Task SendMessagesToEventHub(int numMessagesToSend)
        {
            int j = 0;
            for (var i = 0; i < numMessagesToSend; i++)
            { 
                try
                {
                    var tEvent = new TempratureInfo();

                    tEvent = new TempratureInfo()
                    {
                        Id = i.ToString(),
                        TempratureCelcius = i % 100 == 0 ? 600 : GetRandomNumber(350, 375),
                        SensorId = "H-Sensor-01",
                        EventTime = DateTime.Now
                    };

                    if (i > 200 && i < 220)
                    {
                        j++;

                        tEvent = new TempratureInfo()
                        {
                            Id = i.ToString(),
                            TempratureCelcius = 375 + j * 5,
                            SensorId = "H-Sensor-01",
                            EventTime = DateTime.Now
                        };
                    }

                    string message = JsonConvert.SerializeObject(tEvent);

                    messages.Add(tEvent);

                    Console.WriteLine($"Sending message: {message}");

                    await eventHubClient.SendAsync(
                        new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }

                await Task.Delay(250);
            }

            Console.WriteLine($"{numMessagesToSend} messages sent.");
        }

        public static double GetRandomNumber(double minimum, double maximum)
        {
            Random random = new Random();
            return random.NextDouble() * (maximum - minimum) + minimum;
        }
    }
}