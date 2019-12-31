// using System;
// using static Dexih.Utils.DataType.DataType;
// using Dexih.Utils.DataType;
// using MessagePack;
//
// namespace dexih.functions.Query
// {
//     [MessagePackObject]
//     public class JoinFilter : IEquatable<JoinFilter>
//     {
//         public JoinFilter() { }
//
//        
//         /// <summary>
//         /// Sets a simple filter comparing a column against a static value.
//         /// </summary>
//         /// <param name="column1">Column name from incoming data</param>
//         /// <param name="operator1">Comparison Operator</param>
//         /// <param name="column2">Column to compare with</param>
//         public JoinFilter(TableColumn column1, ECompare operator1, TableColumn column2)
//         {
//             JoinColumn = column1;
//             Operator = operator1;
//             InputColumn = column2;
//
//             if (InputColumn == null)
//                 CompareDataType = ETypeCode.String;
//             else
//                 CompareDataType = column2.DataType;
//         }
//
//         /// <summary>
//         /// Sets a simple filter comparing a column against a static value.
//         /// </summary>
//         /// <param name="column1">Column name from incoming data</param>
//         /// <param name="operator1">Comparison Operator</param>
//         /// <param name="value2">Static value to compare to</param>
//         public JoinFilter(TableColumn column1, ECompare operator1, object value2)
//         {
//             JoinColumn = column1;
//             Operator = operator1;
//             InputValue = value2;
//
//             if (InputValue == null)
//                 CompareDataType = ETypeCode.String;
//             else if(InputValue.GetType().IsArray)
//                 CompareDataType = GetTypeCode(InputValue.GetType().GetElementType(), out _);
//             else
//                 CompareDataType = GetTypeCode(InputValue.GetType(), out _);
//         }
//
//         public JoinFilter(string columnName1, ECompare operator1, object value2)
//         {
//             Operator = operator1;
//             InputValue = value2;
//
//             if (InputValue == null)
//                 CompareDataType = ETypeCode.String;
//             else if (InputValue.GetType().IsArray)
//                 CompareDataType = GetTypeCode(InputValue.GetType().GetElementType(), out _);
//             else
//                 CompareDataType = GetTypeCode(InputValue.GetType(), out _);
//
//             JoinColumn = new TableColumn(columnName1, CompareDataType);
//         }
//
//         public JoinFilter(string columnName1, object value2): this(columnName1, ECompare.IsEqual, value2)
//         {
//         }
//         
//         public JoinFilter(TableColumn column1, object value2): this(column1, ECompare.IsEqual, value2)
//         {
//         }
//
//         /// <summary>
//         /// Sets a simple filter comparing two columns against each other.
//         /// </summary>
//         /// <param name="column1">Column name from incoming data</param>
//         /// <param name="operator1">Comparison Operator</param>
//         /// <param name="column2">Static value to compare to</param>
//         /// <param name="dataType">Data type of the column</param>
//         public JoinFilter(TableColumn column1, ECompare operator1, TableColumn column2, ETypeCode dataType)
//         {
//             JoinColumn = column1;
//             Operator = operator1;
//             InputColumn = column2;
//             CompareDataType = dataType;
//         }
//
//         public JoinFilter(string columnName1, ECompare operator1, string columnName2, ETypeCode dataType)
//         {
//             JoinColumn = new TableColumn(columnName1);
//             Operator = operator1;
//             InputColumn = new TableColumn(columnName2);
//             CompareDataType = dataType;
//         }
//         
//
//         [Key(0)]
//         public TableColumn JoinColumn { get; set; }
//
//         [Key(1)]
//         public object JoinValue { get; set; }
//
//         [Key(2)]
//         public ETypeCode CompareDataType { get; set; }
//
//         [Key(3)]
//         public TableColumn InputColumn { get; set; }
//
//         [Key(4)]
//         public object InputValue { get; set; }
//
//         [Key(5)]
//         public ECompare Operator { get; set; } = ECompare.IsEqual;
//
//         [Key(6)] 
//         public EAndOr AndOr { get; set; } = EAndOr.And;
//
//         public ETypeCode BestDataType()
//         {
//             var typeCode = ETypeCode.String;
//
//             if (JoinColumn != null)
//             {
//                 typeCode = JoinColumn.DataType;
//             } else if (InputColumn != null)
//             {
//                 typeCode = InputColumn.DataType;
//             }
//             else
//             {
//                 typeCode = CompareDataType;
//             }
//
//             return typeCode;
//         }
//
//         public bool Evaluate(object column1Value, object column2Value)
//         {
//             var value1 = JoinColumn == null ? JoinValue : column1Value;
//             var value2 = InputColumn == null ? InputValue : column2Value;
//
//             return Operations.Evaluate(Operator, CompareDataType, value1, value2);
//             
//         }
//
//         public bool Equals(JoinFilter other)
//         {
//             if (ReferenceEquals(null, other)) return false;
//             if (ReferenceEquals(this, other)) return true;
//             return 
//                 Equals(JoinColumn, other.JoinColumn) && 
//                 Equals(JoinValue, other.JoinValue) && 
//                 CompareDataType == other.CompareDataType && 
//                 Equals(InputColumn, other.InputColumn) && 
//                 Equals(InputValue, other.InputValue) && 
//                 Operator == other.Operator && 
//                 AndOr == other.AndOr;
//         }
//
//         public override bool Equals(object obj)
//         {
//             if (ReferenceEquals(null, obj)) return false;
//             if (ReferenceEquals(this, obj)) return true;
//             if (obj.GetType() != GetType()) return false;
//             return Equals((Filter) obj);
//         }
//
//         public override int GetHashCode()
//         {
//             unchecked
//             {
//                 var hashCode = (JoinColumn != null ? JoinColumn.GetHashCode() : 0);
//                 hashCode = (hashCode * 397) ^ (JoinValue != null ? JoinValue.GetHashCode() : 0);
//                 hashCode = (hashCode * 397) ^ (int) CompareDataType;
//                 hashCode = (hashCode * 397) ^ (InputColumn != null ? InputColumn.GetHashCode() : 0);
//                 hashCode = (hashCode * 397) ^ (InputValue != null ? InputValue.GetHashCode() : 0);
//                 hashCode = (hashCode * 397) ^ (int) Operator;
//                 hashCode = (hashCode * 397) ^ (int) AndOr;
//                 return hashCode;
//             }
//         }
//     }
// }