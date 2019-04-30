using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.ML;
using Microsoft.ML.Data;

namespace dexih.functions.ml
{
    public static class Helpers
    {
       
        public static ITransformer LoadModel(MLContext mlContext, byte[] modelBytes)
        {
            using (var stream = new MemoryStream(modelBytes))
            {
                var model = mlContext.Model.Load(stream, out var inputSchema);
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
    }
}