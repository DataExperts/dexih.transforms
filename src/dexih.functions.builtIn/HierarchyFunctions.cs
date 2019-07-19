using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using dexih.functions.Exceptions;

namespace dexih.functions.BuiltIn
{
    public class HierarchyFunctions
    {
        private readonly OrderedDictionary _dictionary;
        private readonly HashSet<object> _createdNodes;
        private bool _parentCreated;

        public HierarchyFunctions()
        {
            _dictionary = new OrderedDictionary();
            _createdNodes = new HashSet<object>();
        }
        [TransformFunction(FunctionType = EFunctionType.Aggregate, Category = "Hierarchy", Name = "Flatten Parent/Child", Description = "Flattens parent/child table into level columns.", ResultMethod = nameof(FlattenParentChildResult), ResetMethod = nameof(Reset), GeneratesRows = true)]
        public void FlattenParentChild(
            [TransformFunctionParameter(Description = "Child Item")] object child, 
            [TransformFunctionParameter(Description = "Parent Item")]object parent)
        {
            if (_dictionary.Contains(child))
            {
                throw new FunctionException("Flatten parent/child failed due to a duplicate child element.", child);
            }
            
            _dictionary.Add(child, (child, parent));
        }

        public bool FlattenParentChildResult([TransformFunctionVariable(EFunctionVariable.Index)]int index, 
            [TransformFunctionParameter(Description = "Maximum iterations to traverse the hierarchy")]int maxDepth,
            [TransformFunctionParameter(Description = "Depth of the levels")]out object leafValue,
            [TransformFunctionParameter(Description = "Depth of the levels")]out object depth, 
            [TransformFunctionParameter(Description = "Array of each flattened level")]out object[] levels )
        {
            if (index >=  _dictionary.Count)
            {
                depth = -1;
                levels = null;
                leafValue = null;
                _parentCreated = false;
                return false;
            }

            var usedValues = new HashSet<object>();

            var currentValue = ((object child, object parent))_dictionary[index];
            usedValues.Add(currentValue.child);
            leafValue = currentValue.child;

            // traverse up the hierarchy adding each node to the used values
            while (currentValue.parent != null && !(currentValue.parent is DBNull))
            {
                // if the parent value doesn't exist as a child, and this is the bottom node, create the parent as a new row.
                if (!_dictionary.Contains(currentValue.parent))
                {
                    if (!_parentCreated && currentValue.child == leafValue && !_createdNodes.Contains(currentValue.parent))
                    {
                        _createdNodes.Add(currentValue.parent);
                        depth = 0;
                        leafValue = currentValue.parent;
                        levels = new object[maxDepth + 1];
                        levels[0] = leafValue;
                        _parentCreated = true;
                        
                        return true;
                    }

                    usedValues.Add(currentValue.parent);

                    break;
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

            _parentCreated = false;

            return false;
        }

        public void Reset()
        {
            _dictionary.Clear();
        }
    }
}