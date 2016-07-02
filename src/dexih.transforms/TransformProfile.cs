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

        bool _lastRecord = false;

        private List<Function> _profiles;

        public Transform ProfileResults { get; protected set; }
        public Transform DistributionResults { get; protected set; }

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
                        var result = input.SetValue(PrimaryTransform[input.ColumnName]);
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
                Table profileResults = new Table("ProfileResults");
                profileResults.Columns.Add(new TableColumn("Profile", DataType.ETypeCode.String));
                profileResults.Columns.Add(new TableColumn("Column", DataType.ETypeCode.String));
                profileResults.Columns.Add(new TableColumn("Result", DataType.ETypeCode.String));

                foreach (Function profile in _profiles)
                {
                    var result = profile.ReturnValue();
                    if (result.Success == false)
                        throw new Exception("Error retrieving aggregate result.  Message: " + result.Message);

                    object[] row = new object[3];
                    row[0] = profile.FunctionName;
                    row[1] = profile.Inputs[0].ColumnName;

                    if(profile.ReturnValue().Success)
                        row[2] = profile.ReturnValue().Value;
                    else
                        row[2] = "Error: " + profile.ReturnValue().Message;

                    profileResults.Data.Add(row);
                }

                ProfileResults = new ReaderMemory(profileResults);

                return new ReturnValue<object[]>(false, null);
            }

        }

        public override string Details()
        {
            return "Profile";
        }
    }
}
