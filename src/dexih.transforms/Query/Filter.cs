using System;
using System.Runtime.Serialization;
using static Dexih.Utils.DataType.DataType;
using Dexih.Utils.DataType;


namespace dexih.functions.Query
{
    [DataContract]
    public class Filter : IEquatable<Filter>
    {
        public Filter() { }

       
        /// <summary>
        /// Sets a simple filter comparing a column against a static value.
        /// </summary>
        /// <param name="column1">Column name from incoming data</param>
        /// <param name="operator1">Comparison Operator</param>
        /// <param name="column2">Column to compare with</param>
        public Filter(TableColumn column1, ECompare operator1, TableColumn column2)
        {
            Column1 = column1;
            Operator = operator1;
            Column2 = column2;

            if (Column2 == null)
                CompareDataType = ETypeCode.String;
            else
                CompareDataType = column2.DataType;
        }

        /// <summary>
        /// Sets a simple filter comparing a column against a static value.
        /// </summary>
        /// <param name="column1">Column name from incoming data</param>
        /// <param name="operator1">Comparison Operator</param>
        /// <param name="value2">Static value to compare to</param>
        public Filter(TableColumn column1, ECompare operator1, object value2)
        {
            Column1 = column1;
            Operator = operator1;
            Value2 = value2;

            if (Value2 == null)
                CompareDataType = ETypeCode.String;
            else if(Value2.GetType().IsArray)
                CompareDataType = GetTypeCode(Value2.GetType().GetElementType(), out _);
            else
                CompareDataType = GetTypeCode(Value2.GetType(), out _);
        }

        public Filter(string columnName1, ECompare operator1, object value2)
        {
            Operator = operator1;
            Value2 = value2;

            if (Value2 == null)
                CompareDataType = ETypeCode.String;
            else if (Value2.GetType().IsArray)
                CompareDataType = GetTypeCode(Value2.GetType().GetElementType(), out _);
            else
                CompareDataType = GetTypeCode(Value2.GetType(), out _);

            Column1 = new TableColumn(columnName1, CompareDataType);
        }

        public Filter(string columnName1, object value2): this(columnName1, ECompare.IsEqual, value2)
        {
        }
        
        public Filter(TableColumn column1, object value2): this(column1, ECompare.IsEqual, value2)
        {
        }

        /// <summary>
        /// Sets a simple filter comparing two columns against each other.
        /// </summary>
        /// <param name="column1">Column name from incoming data</param>
        /// <param name="operator1">Comparison Operator</param>
        /// <param name="column2">Static value to compare to</param>
        /// <param name="dataType">Data type of the column</param>
        public Filter(TableColumn column1, ECompare operator1, TableColumn column2, ETypeCode dataType)
        {
            Column1 = column1;
            Operator = operator1;
            Column2 = column2;
            CompareDataType = dataType;
        }

        public Filter(string columnName1, ECompare operator1, string columnName2, ETypeCode dataType)
        {
            Column1 = new TableColumn(columnName1);
            Operator = operator1;
            Column2 = new TableColumn(columnName2);
            CompareDataType = dataType;
        }
        
        [DataMember(Order = 0)]
        public TableColumn Column1 { get; set; }

        [DataMember(Order = 1)]
        public object Value1 { get; set; }

        [DataMember(Order = 2)]
        public ETypeCode CompareDataType { get; set; }

        [DataMember(Order = 3)]
        public TableColumn Column2 { get; set; }

        [DataMember(Order = 4)]
        public object Value2 { get; set; }

        [DataMember(Order = 5)]
        public ECompare Operator { get; set; } = ECompare.IsEqual;

        [DataMember(Order = 6)] 
        public EAndOr AndOr { get; set; } = EAndOr.And;

        /// <summary>
        /// When either value is null return true
        /// </summary>
        [DataMember(Order = 7)] 
        public bool AllowNull { get; set; } = false;
        
        public ETypeCode BestDataType()
        {
            var typeCode = ETypeCode.String;

            if (Column1 != null)
            {
                typeCode = Column1.DataType;
            } else if (Column2 != null)
            {
                typeCode = Column2.DataType;
            }
            else
            {
                typeCode = CompareDataType;
            }

            return typeCode;
        }

        /// <summary>
        /// Indicates if value2 should be an array (1) or not (0)
        /// </summary>
        /// <returns>0 = value2 is not array, 1 = value 2 is array</returns>
        public int RankValue2()
        {
            return (Operator == ECompare.IsIn || Operator == ECompare.IsNotIn) ? 1 : 0;
        }

        public bool Evaluate(object column1Value, object column2Value)
        {
            var value1 = Column1 == null ? Value1 : column1Value;
            var value2 = Column2 == null ? Value2 : column2Value;

            return Operations.Evaluate(Operator, CompareDataType, value1, value2);
            
        }

        public bool Equals(Filter other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return 
                Equals(Column1, other.Column1) && 
                Equals(Value1, other.Value1) && 
                CompareDataType == other.CompareDataType && 
                Equals(Column2, other.Column2) && 
                Equals(Value2, other.Value2) && 
                Operator == other.Operator && 
                AndOr == other.AndOr;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Filter) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Column1 != null ? Column1.GetHashCode() : 0;
                hashCode = (hashCode * 397) ^ (Value1 != null ? Value1.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (int) CompareDataType;
                hashCode = (hashCode * 397) ^ (Column2 != null ? Column2.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Value2 != null ? Value2.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (int) Operator;
                hashCode = (hashCode * 397) ^ (int) AndOr;
                return hashCode;
            }
        }
    }
}