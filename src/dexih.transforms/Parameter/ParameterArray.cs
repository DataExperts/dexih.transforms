
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.functions.Parameter
{
    [DataContract]
    public class ParameterArray : Parameter
    {
        public ParameterArray()
        {
            Parameters = new List<Parameter>();
        }

        public ParameterArray(string name, ETypeCode dataType, int rank)
	    {
		    Name = name;
		    DataType = dataType;
		    Rank = rank;
		    Parameters = new List<Parameter>();
	    }
	    
	    public ParameterArray(string name, ETypeCode dataType, int rank, List<Parameter> parameters)
	    {
		    Name = name;
		    DataType = dataType;
		    Rank = rank;
		    Parameters = parameters;
	    }
        
        [DataMember(Order = 0)]
	    public List<Parameter> Parameters;

	    public override object Value
	    {
		    get
		    {
			    switch (DataType)
			    {
				    case ETypeCode.Byte:
					    return Parameters.Select(c => (byte) (c.Value??0)).ToArray();
				    case ETypeCode.SByte:
					    return Parameters.Select(c => (sbyte) (c.Value??0)).ToArray();
				    case ETypeCode.UInt16:
					    return Parameters.Select(c => (ushort) (c.Value??0)).ToArray();
				    case ETypeCode.UInt32:
					    return Parameters.Select(c => (int) (c.Value??0)).ToArray();
				    case ETypeCode.UInt64:
					    return Parameters.Select(c => (ulong) (c.Value??0l)).ToArray();
				    case ETypeCode.Int16:
					    return Parameters.Select(c => (short) (c.Value??0)).ToArray();
				    case ETypeCode.Int32:
					    return Parameters.Select(c => (int) (c.Value??0)).ToArray();
				    case ETypeCode.Int64:
					    return Parameters.Select(c => (long) (c.Value??0l)).ToArray();
				    case ETypeCode.Decimal:
					    return Parameters.Select(c => (decimal) (c.Value??0m)).ToArray();
				    case ETypeCode.Double:
					    return Parameters.Select(c => (double) (c.Value??0d)).ToArray();
				    case ETypeCode.Single:
					    return Parameters.Select(c => (float) (c.Value??0d)).ToArray();
				    case ETypeCode.String:
					    return Parameters.Select(c => (string) c.Value).ToArray();
				    case ETypeCode.Boolean:
					    return Parameters.Select(c => (bool) (c.Value??false)).ToArray();
				    case ETypeCode.DateTime:
				    case ETypeCode.Date:
					    return Parameters.Select(c => (DateTime) c.Value).ToArray();
				    case ETypeCode.Time:
					    return Parameters.Select(c => (DateTime) c.Value).ToArray();
				    case ETypeCode.Guid:
					    return Parameters.Select(c => (Guid) c.Value).ToArray();
				    default:
					    return Parameters.Select(c => c.Value).ToArray();
			    }
		    }
		    set { }
	    }

	    public override IEnumerable<SelectColumn> GetRequiredColumns()
	    {
		    return Parameters.SelectMany(c => c.GetRequiredColumns());
	    }
	    
	    public override IEnumerable<TableColumn> GetOutputColumns()
	    {
		    return Parameters.SelectMany(c => c.GetOutputColumns());
	    }

	    public override void InitializeOrdinal(Table table, Table joinTable = null)
	    {
		    foreach (var parameter in Parameters)
		    {
			    parameter.InitializeOrdinal(table, joinTable);
		    }
	    }

	    public TableColumn[] TableColumns()
	    {
		    var columns = new List<TableColumn>();

		    foreach (var parameter in Parameters)
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

		    return columns.ToArray();
	    }

	    public override void SetInputData(object[] data, object[] joinRow = null)
	    {
		    foreach (var parameter in Parameters)
		    {
			    switch (parameter)
			    {
				    case ParameterColumn _:
					case ParameterJoinColumn _:
					case ParameterArray _:
					    parameter.SetInputData(data, joinRow);
					    break;
					    
			    }
		    }
	    }

	    public override void PopulateRowData(object value, object[] row, object[] joinRow = null)
	    {
		    if (value is Array valueArray)
		    {
			    var i = 0;
			    foreach (var parameter in Parameters)
			    {
				    parameter.PopulateRowData(valueArray.GetValue(i), row);
				    i++;
			    }
		    }
	    }

	    public override bool ContainsNullInput(bool throwIfNull)
	    {
		    foreach (var parameter in Parameters)
		    {
			    if (parameter.ContainsNullInput(throwIfNull))
			    {
				    return true;
			    }
		    }

		    return false;
	    }

	    public override Parameter Copy()
	    {
		    return new ParameterArray(Name, DataType, Rank, Parameters.Select(c => c.Copy()).ToList());
	    }
    }
}