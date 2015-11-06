using Microsoft.ServiceBus.Messaging;
using System;
using Microsoft.Azure;

namespace Consumer
{
    class Program
    {

        // Const Variables
        private const String QUEUE_NAME = "queue1";

        // consumes messages from the cloud's queue and prints it to console every 3 seconds
        static void Main(string[] args)
        {
            string connectionString =
                CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");
            QueueClient Client =
              QueueClient.CreateFromConnectionString(connectionString, QUEUE_NAME);

            // Configure the callback options.
            OnMessageOptions options = new OnMessageOptions();
            options.AutoComplete = false;
            options.AutoRenewTimeout = TimeSpan.FromMilliseconds(3000);

            // Callback to handle received messages.
            Client.OnMessage((message) =>
            {
                try
                {
                    // Process message from queue.
                    Console.WriteLine("Body: " + message.GetBody<string>());
                    Console.WriteLine("MessageID: " + message.MessageId);
                    Console.WriteLine("Test Property: " +
                    message.Properties["TestProperty"]);

                    // Remove message from queue.
                    message.Complete();
                }
                catch (Exception)
                {
                    // Indicates a problem, unlock message in queue.
                    message.Abandon();
                }
            }, options);


            Console.ReadKey();
        }
    }
}
