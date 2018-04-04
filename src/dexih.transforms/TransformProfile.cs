using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.transforms.Exceptions;
using dexih.transforms.Transforms;

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

        public TransformProfile(Transform inTransform, List<TransformFunction> profiles)
        {
            _profiles = profiles;
            SetInTransform(inTransform);
        }

        private bool _lastRecord = false;

        private List<TransformFunction> _profiles;

        private Table _profileResults;


        public override Transform GetProfileResults()
        {
            if (_profileResults != null)
                return new ReaderMemory(_profileResults);
            else
                return null;
        }

        public override bool InitializeOutputFields()
        {
            CacheTable = PrimaryTransform.CacheTable.Copy();

            return true;
        }

        public bool SetProfiles(List<TransformFunction> profiles)
        {
            _profiles = profiles;
            return InitializeOutputFields();
        }


        public override bool RequiresSort => false;
        public override bool PassThroughColumns => true;


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
                var newRow = new object[CacheTable.Columns.Count];
                PrimaryTransform.GetValues(newRow);

                foreach (var profile in _profiles)
                {
                    foreach (var input in profile.Inputs.Where(c => c.IsColumn))
                    {
                        try
                        {
                            input.SetValue(PrimaryTransform[input.Column.Name]);
                        } 
                        catch(Exception ex)
                        {
                            throw new TransformException($"The profile transform {Name} failed setting inputs on the function {profile.FunctionName} parameter {input.Name} column {input.Column.Name}.  {ex.Message}", ex, PrimaryTransform[input.Column.Name]);
                        }
                    }

                    try
                    {
                        profile.Invoke();
                    }
                    catch (Exception ex)
                    {
                        throw new TransformException($"The profile transform {Name} failed on the function {profile.FunctionName}.  {ex.Message}", ex);
                    }
                }

                return newRow;
            }
            else
            {

                var profileResults = GetProfileTable("ProfileResults");

                foreach (var profile in _profiles)
                {
                    object profileResult = null;
                    try
                    {
                        profileResult = profile.ReturnValue();
                    }
                    catch (Exception ex)
                    {
                        throw new TransformException($"The profile transform {Name} failed getting the return value on the function {profile.FunctionName}.  {ex.Message}", ex);
                    }

                    var row = new object[6];
                    row[0] = AuditKey;
                    row[1] = profile.FunctionName;
                    row[2] = profile.Inputs[0].Column.Name;
                    row[3] = true;
                    row[4] = profileResult;

                    profileResults.Data.Add(row);

                    if (profile.Outputs.Length > 0)
                    {
                        var details = (Dictionary <string,int>)profile.Outputs[0].Value;

                        if(details != null && details.Count > 0)
                        {
                            foreach(var value  in details.Keys)
                            {
                                row = new object[6];
                                row[0] = AuditKey;
                                row[1] = profile.FunctionName;
                                row[2] = profile.Inputs[0].Column.Name;
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
