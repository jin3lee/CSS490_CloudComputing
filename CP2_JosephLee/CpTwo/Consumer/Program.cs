using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.ServiceBus.Messaging;
using Microsoft.Azure;
using System;

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
                    string bodyMessage = "" + message.GetBody<string>();

                    // Process message from queue
                    Console.WriteLine();
                    Console.WriteLine("Body: " + bodyMessage);
                    Console.WriteLine("MessageID: " + message.MessageId);
                    Console.WriteLine("Test Property: " +
                    message.Properties["TestProperty"]);

                    // write to blob
                    Console.WriteLine("BEFORE retrieveConnectionString() " + bodyMessage);
                    retrieveConnectionString("BODY: " + bodyMessage);
                    Console.WriteLine("AFTER retrieveConnectionString()");

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
        
        public static void retrieveConnectionString(String text)
        {
            Console.WriteLine("CHECK 1");
            //Parse the connection string for the storage account.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                Microsoft.Azure.CloudConfigurationManager.GetSetting("StorageConnectionString"));
            Console.WriteLine("CHECK 2");
            //Create service client for credentialed access to the Blob service.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            Console.WriteLine("CHECK 3");
            //Get a reference to a container.
            CloudBlobContainer container = blobClient.GetContainerReference("my-append-blobs");
            Console.WriteLine("CHECK 4");
            //Create the container if it does not already exist. 
            container.CreateIfNotExists();
            Console.WriteLine("CHECK 5");
            //Get a reference to an append blob.
            CloudAppendBlob appendBlob = container.GetAppendBlobReference("append-blob.log");
            Console.WriteLine("CHECK 6");
            //Create the append blob. Note that if the blob already exists, the CreateOrReplace() method will overwrite it.
            //You can check whether the blob exists to avoid overwriting it by using CloudAppendBlob.Exists().
            if (appendBlob == null)
            {
                System.Console.WriteLine("appendblob == null");
            }
            else
            {
                System.Console.WriteLine("appendblob != null");
            }

            appendBlob.CreateOrReplace();

            int numBlocks = 10;

            //Generate an array of random bytes.
            Random rnd = new Random();
            byte[] bytes = new byte[numBlocks];
            rnd.NextBytes(bytes);

            //Simulate a logging operation by writing text data and byte data to the end of the append blob.
            for (int i = 0; i < numBlocks; i++)
            {
                appendBlob.AppendText(text + " " + i + "/n");
            }

            //Read the append blob to the console window.
            Console.WriteLine(appendBlob.DownloadText());
        }
    }
}
