using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

namespace SequenceGenerator
{
    class Program
    {
        static void Main(string[] args)
        {

            CloudStorageAccount account;
            CloudTableClient client;
            CloudTable table;

            string accountName = "";
            string accountKey = "";

            if (accountName.Length == 0 || accountKey.Length == 0)
            {
                Console.WriteLine("Please provide account name and key");
            }
            else
            {

                account = new CloudStorageAccount(new StorageCredentials(accountName, accountKey), true);
                client = account.CreateCloudTableClient();
                table = client.GetTableReference("Sequence");
                SequenceGenerator sequenceGenerator = new SequenceGenerator();
                //TO test if parallel request occurs
                Parallel.Invoke
                (
                    async () => await sequenceGenerator.GenerateSequenceAsync(table).ConfigureAwait(false),
                    async () => await sequenceGenerator.GenerateSequenceAsync(table).ConfigureAwait(false),
                    async () => await sequenceGenerator.GenerateSequenceAsync(table).ConfigureAwait(false)
                );
            }

            Console.ReadKey();
        }



    }
    public class SequenceGeneratorModel : TableEntity
    {
        public long SequenceId { get; set; }
    }



}
