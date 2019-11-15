using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

namespace SequenceGenerator
{
    public class SequenceGenerator
    {
        public async Task<long> GenerateSequenceAsync(CloudTable table)
        {
            SequenceGeneratorModel sequence = await GetSequenceRecordAsync(table).ConfigureAwait(false);

            if (sequence == null)
            {
                sequence = new SequenceGeneratorModel
                {
                    PartitionKey = "default",
                    RowKey = "rowId",
                    SequenceId = 1
                };
                var insertOperation = TableOperation.Insert(sequence);
                await table.ExecuteAsync(insertOperation).ConfigureAwait(false);
            }
            else
            {
                await RegenerateSequenceAsync(sequence, table).ConfigureAwait(false);
            }

            return sequence.SequenceId;
        }

        private async Task<SequenceGeneratorModel> GetSequenceRecordAsync(CloudTable table)
        {
            string partitionFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "default");
            string rowFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, "rowId");
            string finalFilter = TableQuery.CombineFilters(partitionFilter, TableOperators.And, rowFilter);

            TableQuery<SequenceGeneratorModel> query = new TableQuery<SequenceGeneratorModel>().Where(finalFilter);
            var res = await table.ExecuteQuerySegmentedAsync<SequenceGeneratorModel>(query, null).ConfigureAwait(false);
            return res.FirstOrDefault();
        }

        private async Task<SequenceGeneratorModel> RegenerateSequenceAsync(SequenceGeneratorModel sequence, CloudTable table)
        {
            try
            {
                sequence.SequenceId++;
                TableResult tblr = await table.ExecuteAsync(TableOperation.InsertOrReplace(sequence), null, new OperationContext { UserHeaders = new Dictionary<string, string> { { "If-Match", sequence.ETag } } }).ConfigureAwait(false);
            }
            catch (StorageException ex)
            {
                ////"Optimistic concurrency violation – entity has changed since it was retrieved."
                if (ex.RequestInformation.HttpStatusCode == 412)
                {
                    Thread.Sleep(300);
                    sequence = await this.GetSequenceRecordAsync(table).ConfigureAwait(false);
                    await this.RegenerateSequenceAsync(sequence, table).ConfigureAwait(false);
                }
                else
                {
                    throw;
                }
            }

            return sequence;
        }
    }
}
