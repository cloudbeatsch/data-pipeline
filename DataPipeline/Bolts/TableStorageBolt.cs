using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading.Tasks;
using System.Diagnostics;

namespace DataPipeline.Bolts
{
	public class TableStorageBolt
	{
        public TableStorageBolt(Context context)
        {
            this.Context = context;
        }

		private IDictionary<string, CloudTable> tables = new Dictionary<string, CloudTable>();

        protected Context Context { get; set; }

        public async Task ConnectStorage(params string[] tableNames)
		{
			foreach (var tableName in tableNames)
			{
				if (Properties.Settings.Default.StorageConnectionString != null)
				{
                    try
                    {
                        CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Properties.Settings.Default.StorageConnectionString);
                        CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
                        Context.Logger.Info("Connection Storage String: {0}", Properties.Settings.Default.StorageConnectionString);
                        Context.Logger.Info("Connecting to table storage for: {0}", tableName);

                        try
                        {
                            var curTable = tableClient.GetTableReference(tableName);
                            await curTable.CreateIfNotExistsAsync();

                            tables.Add(tableName, curTable);
                        }
                        catch (Exception e2)
                        {
                            Context.Logger.Error("Failed to create table {0}: {1}", tableName, e2);
                        }
                    }
                    catch (Exception e)
                    {
                        Context.Logger.Error("Failed to init connection: {0}", e);
                    }
				}
			}
		}

		public async Task Insert(TableEntity entity, string tableName = null)
		{
			var table = _GetTable(tableName);
			if (table != null)
			{
                try
                {
                    TableOperation insert = TableOperation.InsertOrReplace(entity);
                    await table.ExecuteAsync(insert);
                }
                catch (Exception e)
                {
                    Context.Logger.Error("Failed to upsert: {0} into {1}", entity, tableName);
                }
			}
			else
			{
				Context.Logger.Warn("Not inserting data into table {0} - table not initialized.", tableName);
			}
		}

		public async Task InsertAll(IEnumerable<TableEntity> entities, string tableName = null)
		{
			var table = _GetTable(tableName);
			if (table != null)
			{
				var inserts = entities.Select(entity => TableOperation.InsertOrReplace(entity));
				var batch = new TableBatchOperation();
				foreach (var insert in inserts)
				{
					batch.Add(insert);
					if (batch.Count == 100)
					{
						await table.ExecuteBatchAsync(batch);
						batch.Clear();
					}
				}
				if (batch.Count > 0)
				{
					await table.ExecuteBatchAsync(batch);
				}
			}
			else
			{
				Context.Logger.Warn("Not inserting batch of data into table {0} - table not initialized.", tableName);
			}
		}
        
		private CloudTable _GetTable(string tableName)
		{
			CloudTable table = null;
			if (tableName == null)
			{
				if (tables.Count == 1)
				{
					table = tables.Values.First();
				}
				else if (tables.Count > 1)
				{
					throw new ArgumentException("Must provide table name if more than one table is initialized");
				}
			}
			else
			{
				tables.TryGetValue(tableName, out table);
			}

			return table;
		}
	}
}