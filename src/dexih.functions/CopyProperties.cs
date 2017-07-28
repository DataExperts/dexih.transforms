using System;
using System.Collections;
using System.Linq;
using System.Reflection;

namespace dexih.functions
{
	[AttributeUsage(AttributeTargets.Property)]
	public class CollectionKeyAttribute : Attribute
	{
	}

	[AttributeUsage(AttributeTargets.Property)]
	public class IsValidAttribute : Attribute
	{
	}

	[AttributeUsage(AttributeTargets.Property)]
	public class ParentCollectionKeyAttribute : Attribute
	{
	}

	[AttributeUsage(AttributeTargets.Property)]
	public class IgnoreCopyAttribute : Attribute
	{
	}

	/// <summary>
	/// A static class for reflection type functions
	/// </summary>
	public static class Reflection
    {
        /// <summary>
        /// Extension for 'Object' that copies the properties to a destination object.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="destination">The destination.</param>
        public static void CopyProperties(this object source, object destination, bool onlyPrimaryProperties = false, object parentKeyValue = null)
        {
			// If any this null throw an exception
			if (source == null || destination == null)
			{
				throw new Exception("Source or/and Destination Objects are null");
			}
            
			// Getting the Types of the objects
            var typeDest = destination.GetType();
            var typeSrc = source.GetType();

            // Iterate the Properties of the source instance and  
            // populate them from their desination counterparts  
            var srcProps = typeSrc.GetProperties();

			// get the collectionKey value first
			object collectionKeyValue = null;
			foreach (var srcProp in srcProps)
			{
				if (srcProp.GetCustomAttribute(typeof(CollectionKeyAttribute), true) != null)
				{
					collectionKeyValue = srcProp.GetValue(source);
				}
			}

			foreach (var srcProp in srcProps)
            {
				var targetProperty = typeDest.GetProperty(srcProp.Name);

				if (targetProperty == null)
				{
					continue;
				}
				if (!srcProp.CanRead)
				{
					continue;
				}
				if (!targetProperty.CanWrite)
				{
					continue;
				}

				if(targetProperty.GetCustomAttribute(typeof(IgnoreCopyAttribute), true) != null)
				{
					continue;
				}

				if (!onlyPrimaryProperties)
				{
					// if the item is a collection, then iterate through each property
				if (srcProp.PropertyType.IsNonStringEnumerable() && srcProp.CanWrite)
					{
						var srcCollection = (IEnumerable)srcProp.GetValue(source, null);
						object targetCollection = (IEnumerable)targetProperty.GetValue(destination, null);
						if (srcCollection == null)
						{
							targetProperty.SetValue(destination, null);
							continue;
						}
						if(targetCollection == null)
						{
							targetCollection = Activator.CreateInstance(targetProperty.PropertyType);
							targetProperty.SetValue(destination, null);

						}
						var addMethod = targetCollection.GetType().GetMethod("Add");

						var typeCollectionArgument = srcCollection.GetType().GetGenericArguments();
						if (typeCollectionArgument.Length > 0)
						{
							var typeCollection = typeCollectionArgument[0];

							var collectionProps = typeCollection.GetProperties();
							PropertyInfo keyAttribute = null;
							PropertyInfo isValidAttribute = null;

							foreach (var prop in collectionProps)
							{
								if (prop != null && prop.GetCustomAttribute(typeof(CollectionKeyAttribute), true) != null)
								{
									keyAttribute = prop;
								}
								if (prop != null && prop.GetCustomAttribute(typeof(IsValidAttribute), true) != null)
								{
									isValidAttribute = prop;
								}
							}

							// if there is an IsValid attribute, set all target items to isvalid = false.  
							if (isValidAttribute != null && keyAttribute != null)
							{
								foreach (var item in (IEnumerable)targetCollection)
								{
									isValidAttribute.SetValue(item, false);
								}
							}

							foreach (var item in srcCollection)
							{
								object targetItem = null;
								object keyvalue = null;
								if (keyAttribute != null)
								{
									keyvalue = keyAttribute.GetValue(item);
									if ((keyvalue is long || keyvalue is int) && (long)keyvalue == 0)
									{

									}
									else
									{
										foreach (var matchItem in (IEnumerable)targetCollection)
										{
											var targetValue = keyAttribute.GetValue(matchItem);
											if (Equals(targetValue, keyvalue))
											{
												if (targetItem != null)
												{
													throw new Exception($"The collections could not be merge due to multiple target key values of {keyvalue} in the collection {typeCollection}.");
												}
												targetItem = matchItem;
											}
										}
									}
								}

								if (targetItem == null)
								{
									targetItem = Activator.CreateInstance(typeCollection);
									item.CopyProperties(targetItem, false, collectionKeyValue);
									addMethod.Invoke(targetCollection, new[] { item });
								}
								else
								{
									item.CopyProperties(targetItem, false, collectionKeyValue);
								}

							}



							//reset all the keyvalues < 0 to 0.  Negative numbers are used to maintain links, but need to be zero before saving datasets to repository.
							if (keyAttribute != null)
							{
								foreach (var item in (IEnumerable)targetCollection)
								{
									var itemValue = keyAttribute.GetValue(item);
									var longValue = Convert.ToInt64(itemValue);
									if (longValue < 0)
									{
										keyAttribute.SetValue(item, 0);
									}
								}
							}
						}
						else 
						{
							targetProperty.SetValue(destination, srcProp.GetValue(source, null), null);
						}
					}
				}

                if(!IsSimpleType(srcProp.PropertyType))
                {
                    continue;
                }
 
                if (targetProperty.GetSetMethod(true) != null && targetProperty.GetSetMethod(true).IsPrivate)
                {
                    continue;
                }
                if ((targetProperty.GetSetMethod().Attributes & MethodAttributes.Static) != 0)
                {
                    continue;
                }
                if (!targetProperty.PropertyType.IsAssignableFrom(srcProp.PropertyType))
                {
                    continue;
                }
				if (targetProperty.GetCustomAttribute(typeof(ParentCollectionKeyAttribute), true) != null && parentKeyValue != null)
				{
					targetProperty.SetValue(destination, parentKeyValue);
					continue;
				}

				// Passed all tests, lets set the value
				targetProperty.SetValue(destination, srcProp.GetValue(source, null), null);
            }
        }

    public static bool IsSimpleType(Type type)
    {
        return
            type.GetTypeInfo().IsPrimitive || type.GetTypeInfo().IsEnum ||
            new[] {
                typeof(Enum),
                typeof(string),
                typeof(decimal),
                typeof(DateTime),
                typeof(DateTimeOffset),
                typeof(TimeSpan),
                typeof(Guid)
            }.Contains(type) ||
            type.GetTypeInfo().BaseType == typeof(Enum) ||
            Convert.GetTypeCode(type) != TypeCode.Object ||
            (type.GetTypeInfo().IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>) && IsSimpleType(type.GetGenericArguments()[0]))
            ;
    }

	// 
	// found at http://stackoverflow.com/questions/3569811/how-to-know-if-a-propertyinfo-is-a-collection
	//

		public static bool IsNonStringEnumerable(this PropertyInfo pi)
	{
		return pi != null && pi.PropertyType.IsNonStringEnumerable();
	}

	public static bool IsNonStringEnumerable(this object instance)
	{
		return instance != null && instance.GetType().IsNonStringEnumerable();
	}

	public static bool IsNonStringEnumerable(this Type type)
	{
		if (type == null || type == typeof(string))
			return false;
		return typeof(IEnumerable).IsAssignableFrom(type);
	}
    }


}