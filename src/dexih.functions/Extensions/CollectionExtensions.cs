using System;
using System.Collections.Generic;
using System.Linq;

namespace dexih.functions
{
    public static class CollectionExtensions
    {
        /// <summary>
        /// Sequence contains same elements (regardless of order)
        /// </summary>
        /// <param name="list1"></param>
        /// <param name="list2"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static bool UnsortedSequenceEquals<T>(this IEnumerable<T> list1, IEnumerable<T> list2) {
            var cnt = new Dictionary<T, int>();
            foreach (var s in list1) {
                if (cnt.ContainsKey(s)) {
                    cnt[s]++;
                } else {
                    cnt.Add(s, 1);
                }
            }
            foreach (var s in list2) {
                if (cnt.ContainsKey(s)) {
                    cnt[s]--;
                } else {
                    return false;
                }
            }
            return cnt.Values.All(c => c == 0);
        }
        
        /// <summary>
        /// Sequence contains same elements (regardless of order) based on the property
        /// </summary>
        /// <param name="list1"></param>
        /// <param name="list2"></param>
        /// <param name="property"></param>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="U"></typeparam>
        /// <returns></returns>
        public static bool UnsortedSequenceEquals<T, U>(this IEnumerable<T> list1, IEnumerable<T> list2, Func<T, U> property)
        {
            var cnt = new Dictionary<U, int>();
            foreach (var s in list1)
            {
                var value = property.Invoke(s);
                if (cnt.ContainsKey(value)) {
                    cnt[value]++;
                } else {
                    cnt.Add(value, 1);
                }
            }
            foreach (var s in list2) {
                var value = property.Invoke(s);
                if (cnt.ContainsKey(value)) {
                    cnt[value]--;
                } else {
                    return false;
                }
            }
            return cnt.Values.All(c => c == 0);
        }
        
        public static bool SequenceContains<T>(this IEnumerable<T> list1, IEnumerable<T> list2) {
            var hash = new HashSet<T>(list1);
            
            foreach (var s in list2) {
                if (!hash.Contains(s)) {
                    return false;
                }
            }

            return true;
        }
        
        public static bool SequenceContains<T, U>(this IEnumerable<T> list1, IEnumerable<T> list2, Func<T, U> property)
        {
            var hash = new HashSet<U>(list1.Select(property.Invoke));
            foreach (var s in list2)
            {
                var value = property.Invoke(s);
                if (!hash.Contains(value)) {
                    return false;
                }
            }

            return true;
        }
        
        public static bool SequenceStartsWith<T>(this IEnumerable<T> list1, IEnumerable<T> list2) {
            using (var e1 = list1.GetEnumerator())
            using (var e2 = list2.GetEnumerator())
            {
                while (e1.MoveNext())
                {
                    if (!e2.MoveNext())
                    {
                        return true;
                    }

                    if (!Equals(e1.Current, e2.Current))
                    {
                        return false;
                    }
                }

                return !e2.MoveNext();
            }
        }
        
        public static bool SequenceStartsWith<T, U>(this IEnumerable<T> list1, IEnumerable<T> list2, Func<T, U> property)
        {
            using (var e1 = list1.GetEnumerator())
            using (var e2 = list2.GetEnumerator())
            {
                while (e1.MoveNext())
                {
                    if (!e2.MoveNext())
                    {
                        return true;
                    }

                    var val1 = property.Invoke(e1.Current);
                    var val2 = property.Invoke(e2.Current);
                    if (!Equals(val1, val2))
                    {
                        return false;
                    }
                }

                return !e2.MoveNext();
            }
        }

        /// <summary>
        /// Adds an item to the collection if it doesn't already exist.
        /// </summary>
        /// <param name="list"></param>
        /// <param name="item"></param>
        /// <typeparam name="T"></typeparam>
        public static void AddIfNotExists<T>(this ICollection<T> list, T item)
        {
            if (!list.Contains(item))
            {
                list.Add(item);
            }
        }
        
        /// <summary>
        /// Adds all items which do not exist in the list.
        /// </summary>
        /// <param name="list"></param>
        /// <param name="items"></param>
        /// <typeparam name="T"></typeparam>
        public static void AddIfNotExists<T>(this ICollection<T> list, IEnumerable<T> items)
        {
            foreach (var item in items)
            {
                if (!list.Contains(item))
                {
                    list.Add(item);
                }
            }
        }
        
        /// <summary>
        /// Adds an item to the collection is another item with the property does not exist. 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="item"></param>
        /// <param name="property"></param>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="U"></typeparam>
        public static void AddIfNotExists<T, U>(this ICollection<T> list, T item,  Func<T, U> property)
        {
            var itemValue = property.Invoke(item);
            foreach(var s in list)
            {
                var listValue = property.Invoke(s);
                if (Equals(itemValue, listValue))
                {
                    return;
                }

            }
            list.Add(item);
        }

        /// <summary>
        /// Adds an item to the collection is another item with the property does not exist. 
        /// </summary>
        /// <param name="list"></param>
        /// <param name="items"></param>
        /// <param name="property"></param>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="U"></typeparam>
        public static void AddIfNotExists<T, U>(this ICollection<T> list, IEnumerable<T> items,  Func<T, U> property)
        {
            var hashSet = new HashSet<U>(list.Select(property.Invoke));
            
            foreach (var item in items)
            {
                var itemValue = property.Invoke(item);
                if (hashSet.Contains(itemValue))
                {
                    continue;
                }
                list.Add(item);
            }
        }
        
    }
}