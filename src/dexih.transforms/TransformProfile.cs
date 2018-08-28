using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Mappings;
using dexih.functions.Parameter;
using dexih.transforms.Exceptions;
using dexih.transforms.Transforms;
using Newtonsoft.Json;

namespace dexih.transforms
{
    [Transform(
        Name = "Profile",
        Description = "Profile incoming data",
        TransformType = TransformAttribute.ETransformType.Profile
    )]
    public class TransformProfile : Transform
    {
        public TransformProfile() {  }

        public TransformProfile(Transform inTransform, Mappings mappings)
        {
            Mappings = mappings;
            SetInTransform(inTransform);
        }

        private bool _lastRecord = false;

//        private List<TransformFunction> _profiles;

        private Table _profileResults;


        public override Transform GetProfileResults()
        {
            if (_profileResults != null)
            {
                return new ReaderMemory(_profileResults);
            }
            else
            {
                return null;
            }
        }

        public override bool InitializeOutputFields()
        {
            CacheTable = PrimaryTransform.CacheTable.Copy();
            return true;
        }

//        public bool SetProfiles(List<TransformFunction> profiles)
//        {
//            _profiles = profiles;
//            return InitializeOutputFields();
//        }


        public override bool RequiresSort => false;

        public override bool ResetTransform()
        {
            _lastRecord = false;
            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            if (_lastRecord)
                return null;

            _lastRecord = !await PrimaryTransform.ReadAsync(cancellationToken);

            if (!_lastRecord)
            {
//                var newRow = new object[CacheTable.Columns.Count];
//                PrimaryTransform.GetValues(newRow);
//
//                foreach (var profile in Mappings.OfType<MapFunction>())
//                {
//                    foreach (var input in profile.Inputs.Where(c => c.IsColumn))
//                    {
//                        try
//                        {
//                            input.SetValue(PrimaryTransform[input.Column.Name]);
//                        } 
//                        catch(Exception ex)
//                        {
//                            throw new TransformException($"The profile transform {Name} failed setting inputs on the function {profile.FunctionName} parameter {input.Name} column {input.Column.Name}.  {ex.Message}", ex, PrimaryTransform[input.Column.Name]);
//                        }
//                    }
//
//                    try
//                    {
//                        profile.Invoke();
//                    }
//                    catch (Exception ex)
//                    {
//                        throw new TransformException($"The profile transform {Name} failed on the function {profile.FunctionName}.  {ex.Message}", ex);
//                    }
//                }

                var newRow = PrimaryTransform.CurrentRow;
                Mappings.ProcessInputData(newRow);
                return newRow;
            }
            else
            {

                var profileResults = GetProfileTable("ProfileResults");

                foreach (var profile in Mappings.OfType<MapFunction>())
                {
                    // createa a dummary row for hte profile function to write to
                    var profileRow = new object[2];
                    
                    // object profileResult = null;
                    try
                    {
                        profile.ProcessResultRow(0, profileRow);
                        // profileResult = profile.ReturnValue;
                    }
                    catch (Exception ex)
                    {
                        throw new TransformException($"The profile transform {Name} failed getting the return value on the function {profile.Function.FunctionName}.  {ex.Message}", ex);
                    }

                    var row = new object[6];
                    row[0] = AuditKey;
                    row[1] = profile.Function.FunctionName;
                    row[2] = profile.Parameters.Inputs.OfType<ParameterColumn>().First().Column.Name;
                    row[3] = true;
                    row[4] = profileRow[1]; //profileResult;

                    profileResults.Data.Add(row);

                    if (profileRow[0] != null)
                    {
                        // var details = (Dictionary <string,int>)profile.Parameters.ResultOutputs.OfType<ParameterColumn>().First().Value;
                        var details = (Dictionary <string,int>)profileRow[0];

                        if(details != null && details.Count > 0)
                        {
                            foreach(var value  in details.Keys)
                            {
                                row = new object[6];
                                row[0] = AuditKey;
                                row[1] = profile.Function.FunctionName;
                                row[2] = profile.Parameters.Inputs.OfType<ParameterColumn>().First().Column.Name;
                                row[3] = false;
                                row[4] = value;
                                row[5] = details[value];

                                profileResults.Data.Add(row);
                            }
                        }
                    }

                    _profileResults = profileResults;
                }

                return null;
            }

        }

        public override string Details()
        {
            return "Profile";
        }
    }
}
