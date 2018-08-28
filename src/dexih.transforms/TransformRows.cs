using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Mappings;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.Transforms;

namespace dexih.transforms
{
    [Transform(
        Name = "Rows",
        Description = "Groups columns and generates rows.",
        TransformType = TransformAttribute.ETransformType.Rows
    )]
    public class TransformRows : Transform
    {
        public TransformRows() { }

        public TransformRows(Transform inTransform, Mappings mappings)
        {
            Mappings = mappings;
//            GroupFields = groupFields;
//            RowFunctions = rowFunctions;
//
            SetInTransform(inTransform, null);
        }

        private bool _firstRecord;

//        private List<string> _passThroughFields;

        //Row generate is used to cache the return value of the row generation functions.  An array is used to allow stacking of row generations.
//        private bool[] _rowGenerate;


//        public List<ColumnPair> GroupFields
//        {
//            get => ColumnPairs;
//            set => ColumnPairs = value;
//        }
//
//        public List<TransformFunction> RowFunctions
//        {
//            get => Functions;
//            set => Functions = value;
//        }

        public override bool InitializeOutputFields()
        {
            CacheTable = Mappings.Initialize(PrimaryTransform.CacheTable);

            
//            CacheTable = new Table("Row");
//
//            var i = 0;
//            if (GroupFields != null)
//            {
//                _firstRecord = true;
//
//                foreach (var groupField in GroupFields)
//                {
//                    var column = PrimaryTransform.CacheTable.Columns[groupField.SourceColumn].Copy();
//                    column.ReferenceTable = "";
//                    column.Name = groupField.TargetColumn.Name;
//                    CacheTable.Columns.Add(column);
//                    i++;
//                }
//            }
//
//            //Initialize the rowgenerate levels to false so first run will run each function.
//            _rowGenerate = new Boolean[RowFunctions.Count];
//            for (var j = 0; j < _rowGenerate.Length; j++)
//                _rowGenerate[j] = false;
//
//            foreach (var rowFunction in RowFunctions)
//            {
//
//                if (rowFunction.Outputs != null)
//                {
//                    foreach (var param in rowFunction.Outputs)
//                    {
//                        var column = new TableColumn(param.Column.Name, param.DataType);
//                        CacheTable.Columns.Add(column);
//                        i++;
//                    }
//                }
//            }
//
//            if (PassThroughColumns)
//            {
//                _passThroughFields = new List<string>();
//
//                foreach (var column in PrimaryTransform.CacheTable.Columns)
//                {
//                    if (CacheTable.Columns.SingleOrDefault(c => c.Name == column.Name) == null)
//                    {
//                        CacheTable.Columns.Add(column.Copy());
//                        _passThroughFields.Add(column.Name);
//                    }
//                }
//            }
//
//            if(GroupFields != null)
//                CacheTable.OutputSortFields = GroupFields.Select(c => new Sort { Column = c.TargetColumn, Direction = Sort.EDirection.Ascending }).ToList();

            _firstRecord = true;
            return true;
        }


        public override Task<bool> Open(Int64 auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;
            if (query == null)
            {
                query = new SelectQuery();
            }

            var groupFields = Mappings.OfType<MapGroup>().ToArray();
            
            // pass through sorts where the column is part of the group field
            if (query.Sorts != null && query.Sorts.Count > 0)
            {
                if (groupFields.Any())
                {
                    var groupNames = groupFields.Select(c => c.InputColumn.Name).ToArray();
                    query.Sorts = query.Sorts.Where(c => c.Column != null && groupNames.Contains(c.Column.Name)).ToList();
                }
                else
                {
                    query.Sorts = null;
                }
            }
            
            // pass through filters where the columns are part of the group fields.
            if (query.Filters != null && query.Filters.Count > 0)
            {
                if (groupFields.Any())
                {
                    var groupNames = groupFields.Select(c => c.InputColumn.Name).ToArray();
                    query.Filters = query.Filters.Where(c =>
                        (c.Column1 != null && groupNames.Contains(c.Column1.Name)) && 
                        (c.Column2 != null &&  groupNames.Contains((c.Column2.Name)))).ToList();
                }
                else
                {
                    query.Filters = null;
                }
            }            

            var returnValue = PrimaryTransform.Open(auditKey, query, cancellationToken);
            return returnValue;
        }


//        public override bool RequiresSort
//        {
//            get
//            {
//                if (GroupFields == null || GroupFields.Count == 0)
//                    return false;
//                else
//                    return true;
//            }
//        }


        public override bool ResetTransform()
        {
            Mappings.Reset();
            
//            foreach (var rowFunction in RowFunctions)
//            {
//                rowFunction.Reset();
//            }

//            for (var j = 0; j < _rowGenerate.Length; j++)
//            {
//                _rowGenerate[j] = false;
//            }

            _firstRecord = true;
            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            if (_firstRecord)
            {
                if(!await PrimaryTransform.ReadAsync(cancellationToken))
                {
                    return null;
                }
            }

            do
            {
                // if the row generation function returns true, then add the row
                if (Mappings.ProcessInputData(PrimaryTransform.CurrentRow))
                {
                    var newRow = new object[FieldCount];
                    Mappings.ProcessOutputRow(newRow);
                    return newRow;
                }
                else
                {
                    if (!await PrimaryTransform.ReadAsync(cancellationToken))
                    {
                        return null;
                    }
                }
            } while (true);

//            var moreRows = true;
//            var newRow = new object[FieldCount];
//
//            //if the top level rowgenerator needs a new record, then read from source
//            if (_rowGenerate[0] == false && await PrimaryTransform.ReadAsync(cancellationToken)== false)
//                return null;
//            do
//            {
//                var i = 0;
//                
//                Mappings.ProcessOutputRow(newRow);
//
//                if (GroupFields != null)
//                {
//                    //map the group fields if it is a new row, otherwise just increment the column counter
//                    foreach (var mapField in GroupFields)
//                    {
//                        newRow[i] = PrimaryTransform[mapField.SourceColumn];
//                        i = i + 1;
//                    }
//                }
//
//                // get the number of output columns for the row functions.
//                var rowFunctionColumns = RowFunctions.Sum(c => c.Outputs.Count(o => o.Column != null));
//
//                var functionColumn = i + rowFunctionColumns; //use the FunctionColumn variable to track the column number as we are going in reverse through the rowfunctions.
//
//                // Run the row generators functions in reverse as the lowest one will repeat, until finished and then cascade up.
//                for (var j = RowFunctions.Count - 1; j >= 0; j--)
//                {
//                    moreRows = true;
//                    var rowFunction = RowFunctions[j];
//                    functionColumn -= rowFunction.Outputs.Count(o => o.Column != null);
//                    
//
//                    foreach (var input in rowFunction.Inputs.Where(c => c.IsColumn))
//                    {
//                        try
//                        {
//                            input.SetValue(PrimaryTransform[input.Column]);
//                        }
//                        catch(Exception ex)
//                        {
//                            throw new TransformException($"The row transform {Name} failed setting an input parameter on {rowFunction.FunctionName} parameter {input.Name}.  {ex.Message}", ex, PrimaryTransform[input.Column.TableColumnName()]);
//                        }
//                    }
//
//                    if (_firstRecord)
//                        rowFunction.Reset();
//
//                    try
//                    {
//                        var invokeresult = rowFunction.Invoke();
//                        _rowGenerate[j] = (bool)invokeresult;
//                    }
//					catch (FunctionIgnoreRowException)
//					{
//						// TODO: This isn't really ignoring the record, just the function.
//						TransformRowsIgnored++;
//						continue;
//					}
//                    catch(Exception ex)
//                    {
//                        throw new TransformException($"The row transform {Name} failed calling the function {rowFunction.FunctionName}.  {ex.Message}.", ex);
//                    }
//
//                    //if the sequence finished.  reset and try again
//                    if (_rowGenerate[j] == false)
//                    {
//                        rowFunction.Reset();
//
//                        try
//                        {
//                            var invokeresult = rowFunction.Invoke();
//                            _rowGenerate[j] = (bool)invokeresult;
//                        }
//						catch (FunctionIgnoreRowException)
//						{
//							// TODO: This isn't really ignoring the record, just the function.
//							TransformRowsIgnored++;
//							continue;
//						}
//                        catch (Exception ex)
//                        {
//                            throw new TransformException($"The row transform {Name} failed calling the function {rowFunction.FunctionName}.  {ex.Message}.", ex);
//                        }
//
//                        moreRows = false; //indicate the row generator finished current sequence and started again.
//                    }
//
//                    if (rowFunction.Outputs != null)
//                    {
//                        var columnNumber = functionColumn;
//                        foreach (var output in rowFunction.Outputs.Where(c=>c.Column != null))
//                        {
//                            newRow[columnNumber++] = output.Value;
//                        }
//                    }
//
//                    //if the sequence hasn't finished, and this isn't the firstrecord of the group, then break.
//                    if (moreRows && _firstRecord == false)
//                        break;
//                }
//
//                _firstRecord = false;
//
//                i = i + rowFunctionColumns;
//
//                if (Mapping.PassThroughColumns)
//                {
//                    foreach (var columnName in _passThroughFields)
//                    {
//                        newRow[i] = PrimaryTransform[columnName];
//                        i = i + 1;
//                    }
//                }
//
//                //if the top level rowgenerator is not at the end of the sequence break to return current row.
//                if (moreRows)
//                    break;
//
//                moreRows = await PrimaryTransform.ReadAsync(cancellationToken);
//                _firstRecord = true;
//                //the rowgenerators have finished, so get the next row.
//            } while (moreRows);
//
//            if (moreRows)
//            {
//                return newRow;
//            }
//            else
//            {
//                return null;
//            }
        }

        public override string Details()
        {
            return "Row Transform";
            // return "Row Transform: " + (Mappings.PassThroughColumns ? "All columns passed through, " : "") + "Grouped Columns:" + (GroupFields?.Count.ToString() ?? "Nill") + ", Series/Aggregate Functions:" + (Functions?.Count.ToString() ?? "Nill");
        }

        public override List<Sort> RequiredSortFields()
        {
            // return GroupFields.Select(c=> new Sort { Column = c.SourceColumn, Direction = Sort.EDirection.Ascending }).ToList();
            return null;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
