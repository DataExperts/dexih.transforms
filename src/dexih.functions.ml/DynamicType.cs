using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Dexih.Utils.DataType;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace dexih.functions.ml
{
    public class DynamicType
    {
	    /// <summary>
	    /// Creates a list of the specified type
	    /// </summary>
	    /// <param name="type"></param>
	    /// <returns></returns>
	    public static IEnumerable<object> CreateDynamicList(Type type)
	    {
		    var listType = typeof(List<>);
		    var dynamicListType = listType.MakeGenericType(type);
		    return (IEnumerable<object>) Activator.CreateInstance(dynamicListType);
	    }

	    /// <summary>
	    /// creates an action which can be used to add items to the list
	    /// </summary>
	    /// <param name="listType"></param>
	    /// <returns></returns>
	    public static Action<object[]> GetAddAction(IEnumerable<object> list, DynamicTypeProperty[] fields)
	    {
		    var listType = list.GetType();
		    var addMethod = listType.GetMethod("Add");
		    var itemType = listType.GenericTypeArguments[0];
		    var itemProperties = itemType.GetProperties();
		    
		    var action = new Action<object[]>((values) =>
		    {
			    var item = Activator.CreateInstance(itemType);

			    for(var i = 0; i < values.Length; i++)
			    {
				    itemProperties[i].SetValue(item, fields[i].Convert(values[i]));
			    }

			    addMethod.Invoke(list, new []{item});
		    });

		    return action;
	    }


	    /// <summary>
	    /// Creates a type based on the property/type values specified in the properties
	    /// </summary>
	    /// <param name="properties"></param>
	    /// <returns></returns>
	    /// <exception cref="Exception"></exception>
        public static Type CreateDynamicType(IEnumerable<DynamicTypeProperty> properties)
        {
            var classCode = new StringBuilder();

            // Generate the class code
            classCode.AppendLine("using System;");
            classCode.AppendLine("namespace Dexih {");
            classCode.AppendLine("public class DynamicClass {");

            foreach (var property in properties)
            {
	            if (property.Type.Name == "ReadOnlyMemory`1")
	            {
		            classCode.AppendLine($"public string {property.CleanName} {{get; set; }}");
	            }
	            else
	            {
		            classCode.AppendLine($"public {property.Type.Name} {property.CleanName} {{get; set; }}");    
	            }
	            
            }
            classCode.AppendLine("}");
            classCode.AppendLine("}");

            var syntaxTree = CSharpSyntaxTree.ParseText(classCode.ToString());

			var references = new MetadataReference[]
			{
				MetadataReference.CreateFromFile(typeof(object).GetTypeInfo().Assembly.Location),
				MetadataReference.CreateFromFile(typeof(DictionaryBase).GetTypeInfo().Assembly.Location)
			};

			var compilation = CSharpCompilation.Create("DynamicClass" + Guid.NewGuid() + ".dll",
				syntaxTrees: new[] {syntaxTree},
				references: references,
				options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

			using (var ms = new MemoryStream())
			{
				var result = compilation.Emit(ms);

				if (!result.Success)
				{
					var failures = result.Diagnostics.Where(diagnostic =>
						diagnostic.IsWarningAsError ||
						diagnostic.Severity == DiagnosticSeverity.Error);

					var message = new StringBuilder();

					foreach (var diagnostic in failures)
					{
						message.AppendFormat("{0}: {1}", diagnostic.Id, diagnostic.GetMessage());
					}

					throw new Exception($"Invalid property definition: {message}.");
				}
				else
				{

					ms.Seek(0, SeekOrigin.Begin);
					var assembly = System.Runtime.Loader.AssemblyLoadContext.Default.LoadFromStream(ms);
					var dynamicType = assembly.GetType("Dexih.DynamicClass");
					return dynamicType;
				}
			}
        }

	    public static object CreateDynamicItem(Type type, IEnumerable values)
	    {
		    var itemProperties = type.GetProperties();
		    var item = Activator.CreateInstance(type);
		    var index = 0;
		    foreach(var value in values)
		    {
			    itemProperties[index].SetValue(item, Operations.Parse(itemProperties[index].PropertyType, value));
			    index++;
		    }

		    return item;
	    }
    }
}