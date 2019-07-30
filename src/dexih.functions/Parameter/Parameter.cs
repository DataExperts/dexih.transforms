using System;
using System.Collections.Generic;
using dexih.functions.Exceptions;
using Dexih.Utils.DataType;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using static Dexih.Utils.DataType.DataType;

namespace dexih.functions.Parameter
{
    public abstract class Parameter
    {
        public abstract void InitializeOrdinal(Table table, Table joinTable = null);
        
        /// <summary>
        /// Set the parameter value to the data.
        /// </summary>
        /// <param name="row"></param>
        public abstract void SetInputData(object[] row, object[] joinRow = null);
        
        /// <summary>
        /// Gets the row position to the current value.
        /// </summary>
        /// <param name="row"></param>
        public abstract void PopulateRowData(object value, object[] row, object[] joinRow = null);

        public abstract Parameter Copy();

        
        /// <summary>
        /// If any parameters are null
        /// </summary>
        /// <param name="throwIfNull">Raise exception if null</param>
        /// <returns>true if null value found</returns>
        public virtual bool ContainsNullInput(bool throwIfNull)
        {
            if (Value == null || Value is DBNull)
            {
                if (throwIfNull)
                {
                    throw new FunctionException(
                        $"The input parameter {Name} has a null value, and the function is set to abend on nulls.");
                }

                return true;
            }

            return false;
        }
        
        /// <summary>
        /// Name for the parameter.  This name must be used when referencing parameters in custom functions.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Parameter datatype
        /// </summary>
        [JsonConverter(typeof(StringEnumConverter))]
        public ETypeCode DataType { get; set; }
        
        public int Rank { get; set; }
        
        public virtual object Value { get; set; }

        /// <summary>
        /// Sets and converts the value to the appropriate type.
        /// </summary>
        /// <param name="input"></param>
        public void SetValue(object input)
        {
            if (DataType == ETypeCode.Unknown || input == null || Equals(input, ""))
            {
                Value = input;
            }
            else
            {
                var result = Operations.Parse(DataType, Rank, input);
                Value = result;
            }
        }

        /// <summary>
        ///  gets input columns required by this parameter.
        /// </summary>
        /// <returns></returns>
        public virtual IEnumerable<TableColumn> GetRequiredColumns()
        {
            return new TableColumn[0];
        }

        /// <summary>
        /// Gets reference columns required by this parameter.
        /// </summary>
        /// <returns></returns>
        public virtual IEnumerable<TableColumn> GetRequiredReferenceColumns()
        {
            return new TableColumn[0];
        }

    }
}