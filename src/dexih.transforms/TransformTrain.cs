//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;
//using dexih.functions;
//using System.Threading;
//using dexih.functions.Mappings;
//using dexih.functions.Parameter;
//using dexih.functions.Query;
//using dexih.transforms.Exceptions;
//using dexih.transforms.Transforms;
//using Newtonsoft.Json;
//
//namespace dexih.transforms
//{
//    [Transform(
//        Name = "Train",
//        Description = "Trains data into models",
//        TransformType = TransformAttribute.ETransformType.Profile
//    )]
//    public class TransformTrain : Transform
//    {
//        public TransformTrain()
//        {
//        }
//
//        public TransformTrain(Transform inTransform, Mappings mappings)
//        {
//            Mappings = mappings;
//            SetInTransform(inTransform);
//        }
//
//        private bool _lastRecord = false;
//
////        private List<TransformFunction> _profiles;
//
//        private Table _profileResults;
//
//
//        public override Transform GetProfileResults()
//        {
//            if (_profileResults != null)
//            {
//                return new ReaderMemory(_profileResults);
//            }
//            else
//            {
//                return null;
//            }
//        }
//
//        public override bool RequiresSort => false;
//
//        public override bool ResetTransform()
//        {
//            _lastRecord = false;
//            return true;
//        }
//
//        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
//        {
//            if (_lastRecord)
//                return null;
//
//            _lastRecord = !await PrimaryTransform.ReadAsync(cancellationToken);
//
//            if (!_lastRecord)
//            {
//                var newRow = PrimaryTransform.CurrentRow;
//                Mappings.ProcessInputData(newRow);
//                return newRow;
//            }
//            else
//            {
//                var profileResults = GetProfileTable("Models");
//
//                foreach (var model in Mappings.OfType<MapFunction>())
//                {
//                    // create a a dummy row for hte profile function to write to
//                    var profileRow = new object[2];
//
//                    // object profileResult = null;
//                    try
//                    {
//                        model.ProcessResultRow(new FunctionVariables(), profileRow, EFunctionType.Profile);
//                        // profileResult = profile.ReturnValue;
//                    }
//                    catch (Exception ex)
//                    {
//                        throw new TransformException(
//                            $"The model transform {Name} failed getting the return value on the function {profile.Function.FunctionName}.  {ex.Message}",
//                            ex);
//                    }
//
//                    var row = new object[6];
//                    row[0] = AuditKey;
//                    row[1] = profile.Function.FunctionName;
//                    row[2] = profile.Parameters.Inputs.OfType<ParameterColumn>().First().Column.Name;
//                    row[3] = true;
//                    row[4] = profileRow[1]; //profileResult;
//
//                    profileResults.Data.Add(row);
//
//                    if (profileRow[0] != null)
//                    {
//                        // var details = (Dictionary <string,int>)profile.Parameters.ResultOutputs.OfType<ParameterColumn>().First().Value;
//                        var details = (Dictionary<string, int>) profileRow[0];
//
//                        if (details != null && details.Count > 0)
//                        {
//                            foreach (var value in details.Keys)
//                            {
//                                row = new object[6];
//                                row[0] = AuditKey;
//                                row[1] = profile.Function.FunctionName;
//                                row[2] = profile.Parameters.Inputs.OfType<ParameterColumn>().First().Column.Name;
//                                row[3] = false;
//                                row[4] = value;
//                                row[5] = details[value];
//
//                                profileResults.Data.Add(row);
//                            }
//                        }
//                    }
//
//                    _profileResults = profileResults;
//                }
//
//                return null;
//            }
//        }
//
//        public override string Details()
//        {
//            return "Profile";
//        }
//    }
//}