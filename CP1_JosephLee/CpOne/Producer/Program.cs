using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceBus;
using System;
using Microsoft.Azure;

namespace Producer
{
    class Program
    {
        // Const Variables
        private const String QUEUE_NAME = "queue1";

        // Other variables
        private static int mProducerId = 0;

        // sends specific messages to the cloud's queue containing producer's id and message id
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                System.Console.WriteLine("Please enter a numeric argument.");
            }

            if (args.Length != 0)
            {
                mProducerId = int.Parse(args[0]);
                System.Console.WriteLine("Your producer number is: " + mProducerId);
            }

            initializeQueue();

            Console.ReadKey();
        }

        // creates a queue if one is not created
        static void initializeQueue()
        {
            // Create the queue if it does not exist already.
            string connectionString =
                CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");

            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(connectionString);
        
            if (!namespaceManager.QueueExists(QUEUE_NAME))
            {
                namespaceManager.CreateQueue(QUEUE_NAME);
                System.Console.WriteLine("Queue Created");
                sendMessageToQueue();
            }
            else
            {
                System.Console.WriteLine("Queue Already Exists");
                sendMessageToQueue();
            }
        }

        // sends messages to the queue 
        static void sendMessageToQueue()
        {
            string connectionString =
            CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");

            QueueClient Client =
                QueueClient.CreateFromConnectionString(connectionString, QUEUE_NAME);

            for (int i = 0; ; i++)
            {
                // Create message, passing a string message for the body.
                BrokeredMessage message = new BrokeredMessage("prod#" + mProducerId + " msg#" + i);

                // Set some addtional custom app-specific properties.
                message.Properties["TestProperty"] = "TestValue";
                message.Properties["Message number"] = i;

                // Send message to the queue.
                Client.Send(message);

                // Print message to console 
                System.Console.WriteLine("\"prod#" + mProducerId + " msg#" + i + "\" was sent!");

                // Wait 1 Second before continuing this loop
                System.Threading.Thread.Sleep(1000);
            }
        }
    
    }
}
