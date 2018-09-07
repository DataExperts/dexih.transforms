using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using dexih.functions.Exceptions;
using Dexih.Utils.CopyProperties;

namespace dexih.functions.BuiltIn
{
    public class HierarchyFunctions
    {
        private OrderedDictionary _dictionary; 

        public HierarchyFunctions()
        {
            _dictionary = new OrderedDictionary();
        }
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Hierarchy", Name = "Flatten Parent/Child", Description = "Flattens parent/child table into level columns.", ResultMethod = nameof(FlattenParentChildResult), ResetMethod = nameof(Reset))]
        public void FlattenParentChild(
            [TransformFunctionParameter(Description = "Parent Item")] object child, 
            [TransformFunctionParameter(Description = "Child Item")]object parent)
        {
            if (_dictionary.Contains(child))
            {
                throw new FunctionException("Flatten parent/child failed due to a duplicate child element.", child);
            }
            
            _dictionary.Add(child, (child, parent));
        }

        public object FlattenParentChildResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, 
            [TransformFunctionParameter(Description = "Maximum iterations to traverse the hierarchy")]int maxDepth, 
            [TransformFunctionParameter(Description = "Depth of the levels")]out object depth, 
            [TransformFunctionParameter(Description = "Array of each flattened level")]out object[] levels )
        {
            if (_dictionary.Count == 0)
            {
                depth = -1;
                levels = null;
                return null;
            }

            var usedValues = new HashSet<object>();

            var currentValue = ((object child, object parent))_dictionary[index];
            usedValues.Add(currentValue.child);
            var returnValue = currentValue.child;

            while (currentValue.parent != null && !(currentValue.parent is DBNull))
            {
                if (!_dictionary.Contains(currentValue.parent))
                {
                    throw new FunctionException("Flatten parent/child failed a parent element could not be matched.", currentValue.parent);
                }

                currentValue = ((object child, object parent)) _dictionary[currentValue.parent];

                if (usedValues.Contains(currentValue.child))
                {
                    throw new FunctionException("Flatten parent/child failed as a recursive parent-child relationship was found.", currentValue.child);
                }
                
                usedValues.Add(currentValue.child);
            }


            depth = usedValues.Count - 1;
            levels = new object[maxDepth+1];

            var pos = 0;
            foreach(var value in usedValues.Reverse())
            {
                levels[pos++] = value;
                if (pos >= maxDepth+1)
                {
                    break;
                }
            }
            
            return returnValue;
        }

        public void Reset()
        {
            _dictionary.Clear();
        }
    }
}