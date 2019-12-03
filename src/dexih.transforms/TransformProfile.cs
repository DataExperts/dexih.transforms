using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Parameter;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;

namespace dexih.transforms
{
    [Transform(
        Name = "Profile",
        Description = "Profile incoming data",
        TransformType = ETransformType.Profile
    )]
    public class TransformProfile : Transform
    {
        public TransformProfile() {  }

        public TransformProfile(Transform inTransform, Mappings mappings)
        {
            SetInTransform(inTransform);
            _profileMappings = mappings;
            _profileMappings.Initialize(inTransform.CacheTable);
        }

        private bool _lastRecord = false;

//        private List<TransformFunction> _profiles;

        private readonly Mappings _profileMappings;

        private Table _profileResults;

        public override string TransformName { get; } = "Profile";
        
        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }

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

        public override bool RequiresSort => false;

        public override bool ResetTransform()
        {
            _lastRecord = false;
            return true;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            await _profileMappings.Open(PrimaryTransform.CacheTable, ReferenceTransform?.CacheTable);
            await PrimaryTransform.Open(auditKey, selectQuery, cancellationToken);

            IsOpen = true;
            AuditKey = auditKey;
            SelectQuery = selectQuery;
            return true;

        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            if (_lastRecord)
                return null;

            _lastRecord = !await PrimaryTransform.ReadAsync(cancellationToken);

            if (!_lastRecord)
            {
                var newRow = PrimaryTransform.CurrentRow;
                await _profileMappings.ProcessInputData(newRow, cancellationToken);
                return newRow;
            }
            else
            {

                var profileResults = GetProfileTable("ProfileResults");

                foreach (var profile in _profileMappings.OfType<MapFunction>())
                {
                    // create a a dummy row for hte profile function to write to
                    var profileRow = new object[2];
                    
                    // object profileResult = null;
                    try
                    {
                        await profile.ProcessResultRowAsync(new FunctionVariables(), profileRow, EFunctionType.Profile, cancellationToken);
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
                    row[4] = profileRow[1];

                    profileResults.AddRow(row);

                    // the dictionary contains the distribution analysis.
                    if(profileRow[0] is Dictionary<string, int> details)
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

                            profileResults.AddRow(row);
                        }
                    }

                    _profileResults = profileResults;
                }

                return null;
            }

        }

    }
}
