using System.Collections.Generic;
using System.IO;
using System.Linq;
using dexih.functions.Exceptions;
using Microsoft.ML;
using Microsoft.ML.Data;

namespace dexih.functions.ml
{
    public enum EEncoding {
        None = 1,
        Label,
        HotEncode,
        FeaturizeText
    }
    
    public static class Helpers
    {

        
        public const string PredictedLabel = "Label";

        public static DynamicTypeProperty NewDynamicTypeProperty(string label, EEncoding? encoding)
        {
            switch (encoding)
            {
                case EEncoding.None:
                    return new DynamicTypeProperty(label, typeof(float), encoding);
                case EEncoding.Label:
                    return new DynamicTypeProperty(label, typeof(string), EEncoding.Label);
                case EEncoding.HotEncode:
                    return new DynamicTypeProperty(label, typeof(string), encoding);
            }

            return null;
        }
       
        public static DynamicList AddData(this DynamicList dynamicList, string[] labels, object[] values, EEncoding[] encoding, object predictorValue = null, EEncoding? predictorEncoding = null)
        {
            if (dynamicList == null)
            {
                var baseFields = labels.Select((label, index) => NewDynamicTypeProperty(label, encoding[index]));
                DynamicTypeProperty[] fields;

                if (predictorEncoding == null)
                {
                    fields = baseFields.ToArray();
                }
                else
                {
                    fields = baseFields.Append(NewDynamicTypeProperty(PredictedLabel, predictorEncoding)).ToArray();
                }
                    
                dynamicList = new DynamicList(fields);
            }

            if (predictorEncoding == null)
            {
                dynamicList.Add(values.Select(c=> c).ToArray());
            }
            else
            {
                dynamicList.Add(values.Select(c=> c).Append(predictorValue).ToArray());    
            }

            return dynamicList;
        }
        
        public static ITransformer LoadModel(MLContext mlContext, byte[] modelBytes, out DataViewSchema inputSchema)
        {
            using (var stream = new MemoryStream(modelBytes))
            {
                var model = mlContext.Model.Load(stream, out inputSchema);
                return model;
            }
        }

        public static byte[] SaveModel(MLContext mlContext, DataViewSchema schema, ITransformer model)
        {
            using (var stream = new MemoryStream())
            {
                mlContext.Model.Save(model, schema, stream);
                return stream.ToArray();
            }
        }
        
        public static string[] ImportModelLabels(byte[] model)
        {
            if (model == null)
            {
                throw new FunctionException("The model is set to null, ensure that this is specified before selecting import.");
            }
            
            var mlContext = new MLContext();
            LoadModel(mlContext, model, out var inputSchema);
            var columns = inputSchema.Select(c => c.Name).Where(c=> c != PredictedLabel).ToArray();
            return columns;

        }

        /// <summary>
        /// Creates a pipeline with the necessary encodings
        /// </summary>
        /// <param name="mlContext"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        public static IEstimator<ITransformer> CreatePipeline(MLContext mlContext, IEnumerable<DynamicTypeProperty> fields, string featuresColumnName, bool includePredictedLabel = true)
        {
            IEstimator<ITransformer> pipeline = null;

            if (includePredictedLabel)
            {
                pipeline = mlContext.Transforms.CopyColumns(outputColumnName: "Label", inputColumnName: PredictedLabel);
            }

            var labels = new List<string>();

            foreach(var field in fields.Where(c => c.Encoding == EEncoding.HotEncode))
            {
                var outColumn = field.Name + "Encoded";
                var estimator = mlContext.Transforms.Categorical.OneHotEncoding(outColumn, field.Name);
                if (pipeline == null)
                {
                    pipeline = estimator;
                }
                else
                {
                    pipeline = pipeline.Append(estimator);
                }
                labels.Add(outColumn);
            }
            
            foreach(var field in fields.Where(c => c.Encoding == EEncoding.FeaturizeText))
            {
                var outColumn = field.Name + "Featurized";
                var estimator = mlContext.Transforms.Text.FeaturizeText(outColumn, field.Name);
                if (pipeline == null)
                {
                    pipeline = estimator;
                }
                else
                {
                    pipeline = pipeline.Append(estimator);
                }
                labels.Add(outColumn);
            }
            
            labels.AddRange(fields.Where(c => c.Encoding == EEncoding.None).Select(c => c.Name) );
            
            var featuresEstimator = mlContext.Transforms.Concatenate(featuresColumnName, labels.ToArray());
            
            if (pipeline == null)
            {
                pipeline = featuresEstimator;
            }
            else
            {
                pipeline = pipeline.Append(featuresEstimator);
            }

            return pipeline;
        }
    }
}