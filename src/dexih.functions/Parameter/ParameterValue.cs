using Dexih.Utils.DataType;
using MessagePack;

namespace dexih.functions.Parameter
{
    [MessagePackObject]
    public class ParameterValue : Parameter
    {
         public ParameterValue()
        {
        }

         /// <summary>
         /// Initializes are parameter
         /// </summary>
         /// <param name="name">Paramter name</param>
         /// <param name="parameterType">Parameter datatype</param>
         /// <param name="value">Value for the parameter (note: requires isColumn = false)</param>
         public ParameterValue(
            string name, 
            ETypeCode parameterType, 
            object value 
            )
        {
            Name = name;
            DataType = parameterType;
            if (value != null)
            {
                SetValue(value);
            }
        }

        public override void InitializeOrdinal(Table table, Table joinTable = null)
        {
        }

        public override void SetInputData(object[] data, object[] joinRow = null)
        {
        }

        public override void PopulateRowData(object value, object[] data, object[] joinRow = null)
        {
        }
        
        public override Parameter Copy()
        {
            return new ParameterValue(Name, DataType, Value);
        }


    }
}