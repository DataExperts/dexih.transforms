using System;
using System.Collections.Generic;
using System.Linq;
using dexih.functions.Query;

namespace dexih.functions.BuiltIn
{
    public class ArrayFunctions
    {
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "ValueAt",
            Description = "Get a value from a specific position in a single dimensional array.")]
        public object ValueAt(int position, object[] values)
        {
            return values[position];
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Length",
            Description = "The number of elements in the array.")]
        public object ArrayLength(object[] values)
        {
            return values.Length;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Concatenate",
            Description = "Concatenate two arrays together")]
        public object[] ArrayConcat(object[] array1, object[] array2)
        {
            return array1.Concat(array2).ToArray();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Search",
            Description = "Returns any values the contain a part of the search string.")]
        public object[] ArraySearch(string search, object[] values)
        {
            var result = values.Where(c => c.ToString().Contains(search)).ToArray();
            return result;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Sort",
            Description = "Sorts the array elements.")]
        public object[] ArraySort(object[] values, Sort.EDirection sortDirection)
        {
            if (sortDirection == Sort.EDirection.Ascending)
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
            Description = "Creates an array.")]
        public object[] CreateArray(object[] values) => values;

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Create Range",
            Description = "Creates an array populated with values from the start, incrementing by 1 'count' times.")]
        public int[] CreateRange(int start, int count)
        {
            return Enumerable.Range(start, count).ToArray();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Array", Name = "Create Sequence",
            Description = "Creates an array populated with values from the start, incrementing by 1 'count' times.")]
        public double[] CreateSequence(double start, double end, double increment)
        {
            if (start > end && increment < 0)
            {
                throw new Exception($"Create range failed, as the start ({start}) is greater than the end ({end}) and the increment ({increment}) is less than zero.");
            }
            if (start < end && increment > 0)
            {
                throw new Exception($"Create range failed, as the start ({start}) is less than the end ({end}) and the increment ({increment}) is greater than zero.");
            }

            var values = new List<double>();

            for (var i = start; i < end; i += increment)
            {
                values.Add(i);
            }

            return values.ToArray();
        }
        
    }
}