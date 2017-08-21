using dexih.functions;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms
{
    /// <summary>
    /// A source transform that uses a prepopulated Table as an input.
    /// </summary>
    public class ReaderPoco<T> : Transform
    {
        private readonly IEnumerable<T> _items;
        private readonly IEnumerator<T> _enumerator;
        private readonly List<Tuple<PropertyInfo, int>> _fieldMappings;
        
        public override ECacheMethod CacheMethod
        {
            get => ECacheMethod.PreLoadCache;
            protected set => throw new Exception("Cache method is always PreLoadCache in the DataTable adapater and cannot be set.");
        }

        #region Constructors
        public ReaderPoco(IEnumerable<T> items)
        {
            _items = items;
            _enumerator = _items.GetEnumerator();
            _fieldMappings = new List<Tuple<PropertyInfo, int>>();

            CacheTable = new Table(typeof(T).Name);

            var position = 0;
            foreach(var propertyInfo in typeof(T).GetProperties())
            {
                var field = propertyInfo.GetCustomAttribute<FieldAttribute>(false) ?? new FieldAttribute(propertyInfo.Name);

                if (field.DeltaType != TableColumn.EDeltaType.IgnoreField)
                {
                    var column = new TableColumn(field.Name)
                    {
                        DeltaType = field.DeltaType,
                        Datatype = DataType.GetTypeCode(propertyInfo.PropertyType),
                        MaxLength = field.MaxLength,
                        Precision = field.Precision,
                        Scale = field.Scale,
                    };

                    CacheTable.Columns.Add(column);
                    _fieldMappings.Add(new Tuple<PropertyInfo, int>(propertyInfo, position));
                    position++;
                }
            }
            
            Reset();

            var table = typeof(T).GetTypeInfo().GetCustomAttribute<TableAttribute>(false);
            if (table != null)
            {
                SortFields = table.SortFields;
                CacheTable.Name = table.Name;
            }
        }

        public override List<Sort> SortFields { get; }


        #endregion

        public override bool InitializeOutputFields()
        {
            return true;
        }

        public override string Details()
        {
            return "Source Table " + CacheTable.Name;
        }

        public override ReturnValue ResetTransform()
        {
            CurrentRowNumber = -1;
            return new ReturnValue(true);
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            return await Task.Run(() =>
            {
                if (_enumerator.MoveNext())
                {
                    var item = _enumerator.Current;
                    var row = new object[_fieldMappings.Count];
                    foreach (var mapping in _fieldMappings)
                    {
                        row[mapping.Item2] = mapping.Item1.GetValue(item);
                    }
                    
                    return new ReturnValue<object[]>(true, row);
                }
                else
                {
                    return new ReturnValue<object[]>(false, null);
                }
                
            }, cancellationToken);
        }
    }
}
