using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using Microsoft.ML;

namespace dexih.functions.ml
{
    public class DynamicList
    {
        public Type Type { get; set; }
        public string[] Labels { get; set; }

        private Action<object[]> _addAction;
        private IEnumerable<object> _data;
        
        public DynamicList(string[] labels, Type defaultType)
        {
            Labels = labels;
            var fields = labels.Select((c, index) => new DynamicTypeProperty(c, defaultType)).ToList();
            Type = DynamicType.CreateDynamicType(fields);
            _data = DynamicType.CreateDynamicList(Type);
            _addAction = DynamicType.GetAddAction(_data);
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