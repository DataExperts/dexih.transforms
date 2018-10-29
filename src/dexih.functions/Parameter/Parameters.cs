using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using dexih.functions.Exceptions;
using Dexih.Utils.CopyProperties;

namespace dexih.functions.Parameter
{
    public class Parameters
    {
        public Parameter ReturnParameter { get; set; }
        public IList<Parameter> Inputs { get; set; }
        public IList<Parameter> Outputs { get; set; }
        public IList<Parameter> ResultInputs { get; set; }
        public IList<Parameter> ResultOutputs { get; set; }
        public Parameter ResultReturnParameter { get; set; }

        public Parameters()
        {
        }
        
        public Parameters(IList<Parameter> inputs, Table table, Table joinTable = null)
        {
            InitializeInputs(inputs, table);
        }

        public void InitializeInputs(IList<Parameter> inputs, Table table, Table joinTable = null)
        {
            Inputs = inputs;
            InitializeColumns(table, joinTable);            
        }

        public void InitializeColumns(Table table, Table joinTable)
        {
            if (Inputs != null)
            {
                foreach (var input in Inputs)
                {
                    input.InitializeOrdinal(table, joinTable);
                }
            } 
            
            if (ResultInputs != null)
            {
                foreach (var input in ResultInputs)
                {
                    input.InitializeOrdinal(table, joinTable);
                }
            }    
        }

        public void InitializeOutputs(Parameter returnParameter, IList<Parameter> outputs, Table table)
        {
            Outputs = outputs;
            ReturnParameter = returnParameter;
            InitializeOutputOrdinals(table);            
        }

        public void InitializeOutputOrdinals(Table table)
        {
            if (Outputs != null)
            {
                foreach (var output in Outputs)
                {
                    output.InitializeOrdinal(table);
                }
            }

            ReturnParameter?.InitializeOrdinal(table);
            
            if (ResultOutputs != null)
            {
                foreach (var output in ResultOutputs)
                {
                    output.InitializeOrdinal(table);
                }
            }

            ResultReturnParameter?.InitializeOrdinal(table);
        }

        public TableColumn[] OutputTableColumns(Parameter returnParameter, ICollection<Parameter> outputs)
        {
            var columns = new List<TableColumn>();

            AddOutputColumn(returnParameter, columns);

            if (outputs != null)
            {
                foreach (var output in outputs)
                {
                    AddOutputColumn(output, columns);
                }
            }

            return columns.ToArray();
        }

        private void AddOutputColumn(Parameter parameter, List<TableColumn> columns)
        {
            if (parameter != null)
            {
                if (parameter is ParameterColumn parameterColumn)
                {
                    columns.Add(parameterColumn.Column);
                }

                if (parameter is ParameterArray parameterArray)
                {
                    columns.AddRange(parameterArray.TableColumns());
                }
            }
        }

        
        /// <summary>
        /// Updates the parameter values with a row of data.
        /// </summary>
        /// <param name="data"></param>
        public void SetFromRow(object[] data, object[] joinData = null)
        {
            if (Inputs == null)
            {
                return;
            }
            
            foreach (var parameter in Inputs)
            {
                parameter.SetInputData(data, joinData);
            }
        }

        /// <summary>
        /// Populates the parameter
        /// </summary>
        /// <param name="returnValue"></param>
        /// <param name="parameterValues"></param>
        /// <param name="outputRow"></param>
        public void SetFunctionResult(object returnValue, object[] parameterValues, object[] outputRow)
        {
            ReturnParameter?.PopulateRowData(returnValue, outputRow);

            var outputIndex = 0; // Inputs?.Count??0;

            if (Outputs != null)
            {
                foreach (var parameter in Outputs)
                {
                    parameter.PopulateRowData(parameterValues[outputIndex], outputRow);
                    outputIndex++;
                }
            }
        }
        
        /// <summary>
        /// Populates the parameter
        /// </summary>
        /// <param name="returnValue"></param>
        /// <param name="parameterValues"></param>
        /// <param name="outputRow"></param>
        public void SetResultFunctionResult(object returnValue, object[] parameterValues, object[] outputRow)
        {
            ResultReturnParameter?.PopulateRowData(returnValue, outputRow);

            var outputIndex = 0; // ResultInputs?.Count??0;

            if (ResultOutputs != null)
            {
                foreach (var parameter in ResultOutputs)
                {
                    parameter.PopulateRowData(parameterValues[outputIndex], outputRow);
                    outputIndex++;
                }
            }
        }
        
        public object[] GetFunctionParameters()
        {
            var count = Inputs?.Count??0 + Outputs?.Count??0;
            var value = new object[count];

            var pos = 0;
            if (Inputs != null)
            {
                foreach (var input in Inputs)
                {
                    value[pos] = input.Value;
                    pos++;
                }
            }

            return value;
        }

        public object[] GetResultFunctionParameters(int? index = null)
        {
            var count = ResultInputs?.Count??0 + ResultOutputs?.Count??0;
            var value = new object[count];

            var pos = 0;
            if (index != null)
            {
                value[pos] = index;
                pos++;
            }
            
            if (ResultInputs != null)
            {
                foreach(var input in ResultInputs)
                {
                    value[pos] = input.Value;
                    pos++;
                }
            }

            return value;
        }
        
        public bool ContainsNullInput(bool throwIfNull)
        {
            foreach (var input in Inputs)
            {
                if (input.Value == null || input.Value is DBNull)
                {
                    throw new FunctionException($"The input parameter {input.Name} has a null value, and the function is set to abend on nulls.");
                }
            }

            return false;
        }

        public Parameters Copy()
        {
            var parameters = new Parameters()
            {
                Inputs = Inputs?.Select(c=> c.Copy()).ToArray(),
                Outputs = Outputs?.Select(c=>c.Copy()).ToArray(),
                ResultInputs =  ResultInputs?.Select(c=>c.Copy()).ToArray(),
                ResultOutputs = ResultOutputs?.Select(c=>c.Copy()).ToArray(),
                ReturnParameter = ReturnParameter?.Copy(),
                ResultReturnParameter = ResultReturnParameter?.Copy()
            };

            return parameters;
        }
    }
}