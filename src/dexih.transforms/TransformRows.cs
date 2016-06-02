using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms
{
    public class TransformRows : Transform
    {
        readonly Dictionary<int, string> _fieldNames = new Dictionary<int, string>();
        readonly Dictionary<string, int> _fieldOrdinals = new Dictionary<string, int>();
        int _fieldCount;

        bool _firstRecord;

        List<string> _passThroughFields;

        //Row generate is used to cache the return value of the row generation functions.  An array is used to allow stacking of row generations.
        private bool[] _rowGenerate;


        public List<ColumnPair> GroupFields
        {
            get
            {
                return ColumnPairs;
            }
            set
            {
                ColumnPairs = value;
            }
        }

        public List<Function> RowFunctions
        {
            get
            {
                return Functions;
            }
            set
            {
                Functions = value;
            }
        }

        public override bool Initialize()
        {
            _fieldNames.Clear();
            _fieldOrdinals.Clear();

            int i = 0;
            if (GroupFields != null)
            {
                _firstRecord = true;

                foreach (ColumnPair groupField in GroupFields)
                {
                    _fieldNames.Add(i, groupField.TargetColumn);
                    _fieldOrdinals.Add(groupField.TargetColumn, i);
                    i++;
                }
            }

            //Initialize the rowgenerate levels to false so first run will run each function.
            _rowGenerate = new Boolean[RowFunctions.Count];
            for (int j = 0; j < _rowGenerate.Length; j++)
                _rowGenerate[j] = false;

            foreach (Function rowFunction in RowFunctions)
            {

                if (rowFunction.Outputs != null)
                {
                    foreach (Parameter param in rowFunction.Outputs)
                    {
                        _fieldNames.Add(i, param.ColumnName);
                        _fieldOrdinals.Add(param.ColumnName, i);
                        i++;
                    }
                }
            }

            //if passthrough is set-on load any unused columns to the output.
            if (PassThroughColumns)
            {
                _passThroughFields = new List<string>();

                for (int j = 0; j < Reader.FieldCount; j++)
                {
                    string columnName = Reader.GetName(j);
                    if (_fieldOrdinals.ContainsKey(columnName) == false)
                    {
                        _fieldNames.Add(i, columnName);
                        _fieldOrdinals.Add(columnName, i);
                        i++;
                        _passThroughFields.Add(columnName);
                    }
                }
            }

            _fieldCount = _fieldOrdinals.Count;
            _firstRecord = true;
            return true;
        }

        public bool SetMappings(List<ColumnPair> groupFields, List<Function> rowFunctions)
        {
            GroupFields = groupFields;
            RowFunctions = rowFunctions;

            return Initialize();
        }

        public override int FieldCount => _fieldCount;

        /// <summary>
        /// checks if filter can execute against the database query.
        /// </summary>
        public override bool CanRunQueries
        {
            get
            {
                return RowFunctions.Exists(c => c.CanRunSql == false) && Reader.CanRunQueries;
            }
        }

        public override bool PrefersSort => true;
        public override bool RequiresSort
        {
            get
            {
                if (GroupFields == null || GroupFields.Count == 0)
                    return false;
                else
                    return true;
            }
        }


        public override string GetName(int i)
        {
            return _fieldNames[i];
        }

        public override int GetOrdinal(string columnName)
        {
            if (_fieldOrdinals.ContainsKey(columnName))
                return _fieldOrdinals[columnName];
            return -1;
        }

     //   public override DataTable GetSchemaTable()
     //   {
     //       DataTable schema = new DataTable("SchemaTable")
     //       {
     //           Locale = CultureInfo.InvariantCulture,
     //           MinimumCapacity = _fieldCount
     //       };

     //       schema.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.BaseColumnName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.BaseSchemaName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.BaseTableName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.ColumnName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.ColumnSize, typeof(int)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.DataType, typeof(object)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsAliased, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsExpression, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsKey, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsLong, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.IsUnique, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.NumericPrecision, typeof(short)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.NumericScale, typeof(short)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableColumn.ProviderType, typeof(int)).ReadOnly = true;

     //       schema.Columns.Add(SchemaTableOptionalColumn.BaseCatalogName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.BaseServerName, typeof(string)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.IsAutoIncrement, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.IsHidden, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.IsReadOnly, typeof(bool)).ReadOnly = true;
     //       schema.Columns.Add(SchemaTableOptionalColumn.IsRowVersion, typeof(bool)).ReadOnly = true;

     //       // null marks columns that will change for each row
     //       object[] schemaRow = {
     //               true,					// 00- AllowDBNull
					//null,					// 01- BaseColumnName
					//string.Empty,			// 02- BaseSchemaName
					//string.Empty,			// 03- BaseTableName
					//null,					// 04- ColumnName
					//null,					// 05- ColumnOrdinal
					//int.MaxValue,			// 06- ColumnSize
					//typeof(string),			// 07- DataType
					//false,					// 08- IsAliased
					//false,					// 09- IsExpression
					//false,					// 10- IsKey
					//false,					// 11- IsLong
					//false,					// 12- IsUnique
					//DBNull.Value,			// 13- NumericPrecision
					//DBNull.Value,			// 14- NumericScale
					//(int) DbType.String,	// 15- ProviderType

					//string.Empty,			// 16- BaseCatalogName
					//string.Empty,			// 17- BaseServerName
					//false,					// 18- IsAutoIncrement
					//false,					// 19- IsHidden
					//true,					// 20- IsReadOnly
					//false					// 21- IsRowVersion
			  //};

     //       DataTable schemaTable = Reader.GetSchemaTable();
     //       if(schemaTable == null)
     //           return null;

     //       int i = 0;

     //       foreach (ColumnPair groupField in GroupFields)
     //       {
     //           DataRow row = schemaTable.Select("ColumnName='" + groupField.SourceColumn + "'")[0];
     //           schemaRow[1] = groupField.TargetColumn; // Base column name
     //           schemaRow[4] = groupField.TargetColumn; // Column name
     //           schemaRow[5] = i; // Column ordinal
     //           schemaRow[7] = row["DataType"];
     //           schema.Rows.Add(schemaRow);
     //           i++;
     //       }
     //       foreach (Function rowFunction in RowFunctions)
     //       {
     //           foreach (Parameter param in rowFunction.Outputs)
     //           {
     //               schemaRow[1] = param.ColumnName; // Base column name
     //               schemaRow[4] = param.ColumnName; // Column name
     //               schemaRow[5] = i; // Column ordinal
     //               schemaRow[7] = Type.GetType("System." + param.DataType);

     //               schema.Rows.Add(schemaRow);
     //               i++;
     //           }
     //       }

     //       if (PassThroughColumns)
     //       {
     //           for (int j = 0; j < Reader.FieldCount; j++)
     //           {
     //               string columnName = Reader.GetName(j);
     //               if (_fieldOrdinals.ContainsKey(columnName) == false)
     //               {
     //                   DataRow row = schemaTable.Select("ColumnName='" + columnName + "'")[0];
     //                   schemaRow[1] = columnName; // Base column name
     //                   schemaRow[4] = columnName; // Column name
     //                   schemaRow[5] = i; // Column ordinal
     //                   schemaRow[7] = row["DataType"];
     //                   schema.Rows.Add(schemaRow);
     //                   i++;
     //               }
     //           }
     //       }


     //       return schema;
     //   }

        public override bool ResetValues()
        {
            foreach (Function rowFunction in RowFunctions)
                rowFunction.Reset();
            _firstRecord = true;
            return true;
        }

        protected override bool ReadRecord()
        {
            bool moreRows = true;

            if (_rowGenerate[0] == false)
                CurrentRow = new object[_fieldCount];

            //if the top level rowgenerator needs a new record, then read from source
            if (_rowGenerate[0] == false && Reader.Read() == false)
                return false;
            do
            {
                int i = 0;

                if (GroupFields != null)
                {
                    //map the group fields if it is a new row, otherwise just increment the column counter
                    foreach (ColumnPair mapField in GroupFields)
                    {
                        CurrentRow[i] = Reader[mapField.SourceColumn];
                        i = i + 1;
                    }
                }

                int functionColumn = i + RowFunctions.Count - 1; //use the FunctionColumn variable to track the column number as we are going in reverse through the rowfunctions.

                // Run the row generators functions in reverse as the lowest one will repeat, until finished and then cascade up.
                for (int j = RowFunctions.Count - 1; j >= 0; j--)
                {
                    moreRows = true;
                    Function rowFunction = RowFunctions[j];

                    foreach (Parameter input in rowFunction.Inputs.Where(c => c.IsColumn))
                    {
                        var result = input.SetValue(Reader[input.ColumnName]);
                        if (result.Success == false)
                            throw new Exception("Error setting row function values: " + result.Message);
                    }

                    if (_firstRecord)
                        rowFunction.Reset();

                    var invokeresult = rowFunction.Invoke();
                    if (invokeresult.Success == false)
                        throw new Exception("Error invoking row function: " + invokeresult.Message);
                    _rowGenerate[j] = (bool)invokeresult.Value;

                    //if the sequence finished.  reset and try again
                    if (_rowGenerate[j] == false)
                    {
                        rowFunction.Reset();

                        var invokeresult2 = rowFunction.Invoke();
                        if (invokeresult2.Success == false)
                            throw new Exception("Error invoking row function: " + invokeresult2.Message);
                        _rowGenerate[j] = (bool)invokeresult2.Value;

                        moreRows = false; //indicate the row generator finished current sequence and started again.
                    }

                    if (rowFunction.Outputs != null)
                    {
                        foreach (Parameter output in rowFunction.Outputs)
                        {
                            if (output.ColumnName != "")
                            {
                                CurrentRow[functionColumn] = output.Value;
                                functionColumn = functionColumn - 1;
                            }
                        }
                    }

                    //if the sequence hasn't finished, and this isn't the firstrecord of the group, then break.
                    if (moreRows && _firstRecord == false)
                        break;
                }

                _firstRecord = false;

                i = i + RowFunctions.Count;

                if (_rowGenerate[0] == false && PassThroughColumns)
                {
                    foreach (string columnName in _passThroughFields)
                    {
                        CurrentRow[i] = Reader[columnName];
                        i = i + 1;
                    }
                }

                //if the top level rowgenerator is not at the end of the sequence break to return current row.
                if (moreRows)
                    break;

                moreRows = Reader.Read();
                _firstRecord = true;
                //the rowgenerators have finished, so get the next row.
            } while (moreRows);

            return moreRows;
        }

        public override string Details()
        {
            return "Row Transform: " + (PassThroughColumns ? "All columns passed through, " : "") + "Grouped Columns:" + (GroupFields?.Count.ToString() ?? "Nill") + ", Series/Aggregate Functions:" + (Functions?.Count.ToString() ?? "Nill");
        }

        public override List<Sort> RequiredSortFields()
        {
            return GroupFields.Select(c=> new Sort { Column = c.SourceColumn, Direction = Sort.EDirection.Ascending }).ToList();
        }

        public override List<Sort> RequiredJoinSortFields()
        {
            return null;
        }

        //group will return sorted by the group fields.
        public override List<Sort> OutputSortFields()
        {
            return GroupFields.Select(c => new Sort { Column = c.TargetColumn, Direction = Sort.EDirection.Ascending }).ToList();
        }

        public override Task<ReturnValue> LookupRow(List<Filter> filters)
        {
            throw new NotImplementedException();
        }
    }
}
