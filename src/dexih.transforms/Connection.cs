using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;
using System.Threading;
using dexih.transforms;
using dexih.functions;
using Newtonsoft.Json.Linq;
using System.Diagnostics;
using System.Data.Common;

namespace dexih.transforms
{
    public abstract class Connection
    {

        #region Enums

        public enum EConnectionState
        {
            Broken = 0,
            Open = 1,
            Closed = 2,
            Fetching = 3,
            Connecting = 4,
            Executing = 5
        }

        public enum ECategory
        {
            SqlDatabase = 0,
            NoSqlDatabase = 1,
            File = 2,
            WebService = 3,
            Application = 4
        }

        #endregion

        #region Properties

        public string Name { get; set; }
        public string ServerName { get; set; }
        public bool NtAuthentication { get; set; }
        public string UserName { get; set; } = "";
        public string Password { get; set; } = "";
        public string DefaultDatabase { get; set; }
        public string FileName { get; set; }
        public EConnectionState State { get; set; }

        public bool UseConnectionString { get; set; }
        public string ConnectionString { get; set; }


        #endregion

        #region Abstracts

        //Abstract Properties
        public abstract string ServerHelp { get; } //help text for what the server means for this description
        public abstract string DefaultDatabaseHelp { get; } //help text for what the default database means for this description

        public abstract string DatabaseTypeName { get; }
        public abstract ECategory DatabaseCategory { get; }
        public abstract bool AllowNtAuth { get; }
        public abstract bool AllowUserPass { get; }

        public abstract bool CanBulkLoad { get; }

        //Functions required for managed connection
        public abstract Task<ReturnValue> CreateTable(Table table, bool dropTable = false);
        //public abstract Task<ReturnValue> TestConnection();
        public abstract Task<ReturnValue<int>> ExecuteUpdate(Table table, List<UpdateQuery> queries);
        public abstract Task<ReturnValue<int>> ExecuteDelete(Table table, List<DeleteQuery> queries);
        public abstract Task<ReturnValue<int>> ExecuteInsert(Table table, List<InsertQuery> queries);
        public abstract Task<ReturnValue<int>> ExecuteInsertBulk(Table table, DbDataReader sourceData);
        public abstract Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query);
        public abstract Task<ReturnValue<Transform>> GetTransformReader(Table table, SelectQuery query, Transform referenceTransform = null);
        public abstract Task<ReturnValue> TruncateTable(Table table);

        /// <summary>
        /// If database connection supports direct DbDataReader.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public abstract Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, SelectQuery query = null);

        //Functions required for datapoint.
        public abstract Task<ReturnValue> CreateDatabase(string DatabaseName);
        public abstract Task<ReturnValue<List<string>>> GetDatabaseList();
        public abstract Task<ReturnValue<List<string>>> GetTableList();

        /// <summary>
        /// Interrogates the underlying data to get the Table structure.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="Properties"></param>
        /// <returns></returns>
        public abstract Task<ReturnValue<Table>> GetSourceTableInfo(string tableName, Dictionary<string, object> Properties);

        /// <summary>
        /// Adds any database specific mandatory column to the table object.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="position"></param>
        /// <returns></returns>
        public abstract Task<ReturnValue> AddMandatoryColumns(Table table, int position);

        public Stopwatch WriteDataTimer = new Stopwatch();


        #endregion

        public virtual bool IsValidDatabaseName(string name)
        {
            return true;
        }

        public virtual bool IsValidTableName(string name)
        {
            return true;
        }

        public virtual bool IsValidColumnName(string name)
        {
            return true;
        }



        /// <summary>
        /// Function runs when a data write comments.  This is used to put headers on csv files.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public virtual async Task<ReturnValue> DataWriterStart(Table table)
        {
            return await Task.Run(() => new ReturnValue(true));
        }

        /// <summary>
        /// Function runs when a data write finishes.  This is used to close file streams.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public virtual async Task<ReturnValue> DataWriterFinish(Table table)
        {
            return await Task.Run(() => new ReturnValue(true));
        }

        public async Task<ReturnValue<Table>> GetPreview(Table table, SelectQuery query, int maxMilliseconds, CancellationToken cancellationToken)
        {
            Stopwatch watch = new Stopwatch();
            watch.Start();

            ReturnValue<Transform> returnValue = await GetTransformReader(table, query);
            if (returnValue.Success == false)
                return new ReturnValue<Table>(returnValue.Success, returnValue.Message, returnValue.Exception, null);

            DbDataReader reader = returnValue.Value;

            TableCache dataPreview = new TableCache();

            int count = 0;
            while (reader.Read() && query.Rows != -1 && count < query.Rows && cancellationToken.IsCancellationRequested == false)
            {
                object[] values = new object[reader.FieldCount];
                reader.GetValues(values);
                dataPreview.Add(values);
                count++;

                if (watch.ElapsedMilliseconds > maxMilliseconds)
                    break;
            }

            watch.Stop();
            reader.Dispose();

            Table dataTable = new Table(table.TableName, new TableColumns(query.Columns.Select(c => new TableColumn(c.Column))), dataPreview);

            return new ReturnValue<Table>(true, "", null, dataTable);
        }

    }
}

