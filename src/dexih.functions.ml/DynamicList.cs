using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.ML;

namespace dexih.functions.ml
{
    public class DynamicList
    {
        public Type Type { get; set; }
        private readonly Action<object[]> _addAction;
        private readonly IEnumerable<object> _data;

        public DynamicTypeProperty[] Fields;
        
        
        public DynamicList(string[] labels, Type defaultType)
        {
            var fields = labels.Select((c, index) => new DynamicTypeProperty(c, defaultType)).ToArray();
            Fields = fields;
            Type = DynamicType.CreateDynamicType(fields);
            _data = DynamicType.CreateDynamicList(Type);
            _addAction = DynamicType.GetAddAction(_data, fields);
        }

        public DynamicList(DynamicTypeProperty[] fields)
        {
            Fields = fields;
            Type = DynamicType.CreateDynamicType(fields);
            _data = DynamicType.CreateDynamicList(Type);
            _addAction = DynamicType.GetAddAction(_data, fields);
        }

        
        public void Add(object[] item)
        {
            _addAction.Invoke(item);
        }
        
        
        public IDataView GetDataView(MLContext mlContext)
        {
            var dataType = mlContext.Data.GetType();
            var loadMethodGeneric = dataType.GetMethods().First(method => method.Name =="LoadFromEnumerable" && method.IsGenericMethod);
            var loadMethod = loadMethodGeneric.MakeGenericMethod(Type);
            var trainData = (IDataView) loadMethod.Invoke(mlContext.Data, new[] {_data, null});
            return trainData;
        }

    }
}