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
            string consumerNumber = "0";
            if (args.Length != 0) { consumerNumber = args[0]; }
            string s_startTime = DateTime.Now.ToString("h:mm:ss tt");
            string s_intro = "\nTestVM" + consumerNumber + "Blob.log:\n"
                            + "*****************************************************\n"
                            + "\"*TestVM "+ consumerNumber + "\" started at "+ s_startTime +" *\"\n"
                            + "*****************************************************\n";
            
            // write intro into blob
            retrieveConnectionString(s_intro);

            string s_body = "TestVM" + consumerNumber + " Received: ";
            string s_append = "";

            // Connect to queue
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
                    // store message to object
                    string bodyMessage = "" + message.GetBody<string>();

                    // collect time of message recieved
                    string s_recieveTime = DateTime.Now.ToString("h:mm:ss tt");

                    // write to blob
                    s_append = s_body + bodyMessage + " at " + s_recieveTime + "\n";

                    // Remove message from queue.
                    message.Complete();

                    // append to blob
                    retrieveConnectionString(s_append);

                    // Wait 1 Second before continuing this loop
                    System.Threading.Thread.Sleep(2000);
                }
                catch (Exception)
                {
                    // Indicates a problem, unlock message in queue.
                    message.Abandon();
                }
            }, options);

            Console.ReadKey();
        }
        
        public static void retrieveConnectionString(string text)
        {
            string cleanString = text;
            //Parse the connection string for the storage account.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                Microsoft.Azure.CloudConfigurationManager.GetSetting("StorageConnectionString"));
            //Create service client for credentialed access to the Blob service.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            //Get a reference to a container.
            CloudBlobContainer container = blobClient.GetContainerReference("jin3leecontainer");
            //Create the container if it does not already exist. 
            container.CreateIfNotExists();
            //Get a reference to an append blob.
            CloudAppendBlob appendBlob = container.GetAppendBlobReference("append-blob.log");
            //Create the append blob. Note that if the blob already exists, the CreateOrReplace() method will overwrite it.
            //You can check whether the blob exists to avoid overwriting it by using CloudAppendBlob.Exists().
            if (!appendBlob.Exists())
            {
                appendBlob.CreateOrReplace();
            }
     
            int numBlocks = 10;

            //Generate an array of random bytes.
            Random rnd = new Random();
            byte[] bytes = new byte[numBlocks];
            rnd.NextBytes(bytes);

            //Simulate a logging operation by writing text data and byte data to the end of the append blob.
            appendBlob.AppendText(text);

            //Read the append blob to the console window.
            Console.WriteLine("\n\n" + appendBlob.DownloadText()+"\n\n");
        }
        
    }
}
