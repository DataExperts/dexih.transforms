using System;
using System.Collections.Generic;
using System.Linq;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.functions.BuiltIn
{
    public class ArrayFunctions<T>
    {

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "ValueAt",
            Description = "Get a value from a specific position in a single dimensional array.", GenericType = EGenericType.All)]
        public T ValueAt(int position, T[] values)
        {
            return values[position];
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Length",
            Description = "The number of elements in the array.")]
        public int ArrayLength(object[] values)
        {
            return values.Length;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Concatenate Arrays",
            Description = "Concatenate two arrays together", GenericType = EGenericType.All)]
        public T[] ArrayConcat(T[] array1, T[] array2)
        {
            return array1.Concat(array2).ToArray();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Search",
            Description = "Returns any values the contain a part of the search string.", GenericType = EGenericType.All)]
        public T[] ArraySearch(string search, T[] values)
        {
            var result = values.Where(c => c.ToString().Contains(search)).ToArray();
            return result;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Sort",
            Description = "Sorts the array elements.", GenericType = EGenericType.All)]
        public T[] ArraySort(T[] values, ESortDirection sortSortDirection)
        {
            if (sortSortDirection == ESortDirection.Ascending)
            {
                return values.OrderBy(c => c).ToArray();
            }
            else
            {
                return values.OrderByDescending(c => c).ToArray();
            }
        }

        /// <summary>
        /// This is a dummy function by the front end to create an array.
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Create Array",
            Description = "Creates an array.", GenericType = EGenericType.All)]
        public T[] CreateArray(T[] values) => values;

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Create Range",
            Description = "Creates an array populated with values from the start, incrementing by 1 'count' times.")]
        public int[] CreateRange(int start, int count)
        {
            return Enumerable.Range(start, count).ToArray();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Create Sequence",
            Description = "Creates an array populated with number values from the start, incrementing by 1 'count' times.", GenericType = EGenericType.Numeric)]
        public T[] CreateNumericSequence(T start, T end, T increment)
        {
            if(Operations.GreaterThan(start, end) && Operations.LessThan(increment, default(T)))
            {
                throw new Exception($"Create range failed, as the start ({start}) is greater than the end ({end}) and the increment ({increment}) is less than zero.");
            }

            if (Operations.LessThan(start, end) && Operations.GreaterThan(increment,default(T)))
            {
                throw new Exception($"Create range failed, as the start ({start}) is less than the end ({end}) and the increment ({increment}) is greater than zero.");
            }

            var values = new List<T>();

            for (var i = start; Operations.LessThan(i, end); Operations.Add(i, increment))
            {
                values.Add(i);
            }

            return values.ToArray();
        }
        
    }
}