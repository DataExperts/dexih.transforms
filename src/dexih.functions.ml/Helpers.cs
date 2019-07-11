using System.Collections.Generic;
using System.IO;
using System.Linq;
using dexih.functions.Exceptions;
using Microsoft.ML;
using Microsoft.ML.Data;

namespace dexih.functions.ml
{
    public enum EEncoding {
        None,
        HotEncode,
        FeaturizeText
    }
    
    public static class Helpers
    {

        
        private const string PredictedLabel = "PredictedLabel";
       
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
            var columns = inputSchema.Select(c => c.Name).Where(c=> c != "Label").ToArray();
            return columns;

        }

        /// <summary>
        /// Creates a pipeline with the neccessary encodings
        /// </summary>
        /// <param name="mlContext"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        public static EstimatorChain<ColumnConcatenatingTransformer> CreatePipeline(MLContext mlContext, IEnumerable<DynamicTypeProperty> fields, string featuresColumnName)
        {
            IEstimator<ITransformer> pipeline = mlContext.Transforms.CopyColumns(outputColumnName: "Label", inputColumnName: PredictedLabel);
            var labels = new List<string>();

            foreach(var field in fields.Where(c => c.Encoding == EEncoding.HotEncode))
            {
                var outColumn = field.Name + "Encoded";
                var estimator = mlContext.Transforms.Categorical.OneHotEncoding(outColumn, field.Name);
                pipeline = pipeline.Append(estimator);
                labels.Add(outColumn);
            }
            
            foreach(var field in fields.Where(c => c.Encoding == EEncoding.FeaturizeText))
            {
                var outColumn = field.Name + "Featurized";
                var estimator = mlContext.Transforms.Text.FeaturizeText(outColumn, field.Name);
                pipeline = pipeline.Append(estimator);
                labels.Add(outColumn);
            }
            
            var featuresEstimator = mlContext.Transforms.Concatenate(featuresColumnName, labels.ToArray());
            return pipeline.Append(featuresEstimator);
        }
    }
}