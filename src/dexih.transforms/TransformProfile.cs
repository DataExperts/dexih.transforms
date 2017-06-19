using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;

namespace dexih.transforms
{
    public class TransformProfile : Transform
    {
        public TransformProfile() {  }

        public TransformProfile(Transform inTransform, List<Function> profiles)
        {
            _profiles = profiles;
            SetInTransform(inTransform);
        }

        private bool _lastRecord = false;

        private List<Function> _profiles;

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

        public bool SetProfiles(List<Function> profiles)
        {
            _profiles = profiles;
            return InitializeOutputFields();
        }


        public override bool RequiresSort => false;
        public override bool PassThroughColumns => true;


        public override ReturnValue ResetTransform()
        {
            _lastRecord = false;
            return new ReturnValue(true);
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            if (_lastRecord)
                return new ReturnValue<object[]>(false, null);

            _lastRecord = !await PrimaryTransform.ReadAsync(cancellationToken);

            if (!_lastRecord)
            {
                object[] newRow = new object[CacheTable.Columns.Count];
                PrimaryTransform.GetValues(newRow);

                foreach (Function profile in _profiles)
                {
                    foreach (Parameter input in profile.Inputs.Where(c => c.IsColumn))
                    {
                        var result = input.SetValue(PrimaryTransform[input.Column.ColumnName]);
                        if (result.Success == false)
                            throw new Exception("Error setting mapping values: " + result.Message);
                    }
                    var invokeresult = profile.Invoke();

                    if (invokeresult.Success == false)
                        throw new Exception("Error invoking profile function: " + invokeresult.Message);
                }

                return new ReturnValue<object[]>(true, newRow);
            }
            else
            {

                Table profileResults = CacheTable.GetProfileTable("ProfileResults");

                foreach (Function profile in _profiles)
                {
                    var result = profile.ReturnValue();
                    if (result.Success == false)
                        throw new Exception("Error retrieving profile result.  Message: " + result.Message);

                    object[] row = new object[6];
                    row[0] = AuditKey;
                    row[1] = profile.FunctionName;
                    row[2] = profile.Inputs[0].Column.ColumnName;
                    row[3] = true;

                    if (profile.ReturnValue().Success)
                        row[4] = profile.ReturnValue().Value;
                    else
                        row[4] = "Error: " + profile.ReturnValue().Message;

                    profileResults.Data.Add(row);

                    if (profile.Outputs.Length > 0)
                    {
                        Dictionary<string, int> details = (Dictionary <string,int>)profile.Outputs[0].Value;

                        if(details != null && details.Count > 0)
                        {
                            foreach(string value  in details.Keys)
                            {
                                row = new object[6];
                                row[0] = AuditKey;
                                row[1] = profile.FunctionName;
                                row[2] = profile.Inputs[0].Column.ColumnName;
                                row[3] = false;
                                row[4] = value;
                                row[5] = details[value];

                                profileResults.Data.Add(row);
                            }
                        }
                    }

                    _profileResults = profileResults;
                }

                return new ReturnValue<object[]>(false, null);
            }

        }

        public override string Details()
        {
            return "Profile";
        }
    }
}
