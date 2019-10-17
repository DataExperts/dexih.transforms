using System;
using System.Collections.Generic;
using MessagePack;

namespace dexih.functions
{
 [MessagePackObject]
    public class NamingStandards : Dictionary<string, string>
    {
        public void LoadDefault()
        {
                AddIfMissing("General.Table.Name",  "{0}" );
                AddIfMissing("Stage.Table.Name",  "stg{0}" );
                AddIfMissing("Validate.Table.Name",  "val{0}" );
                AddIfMissing("Transform.Table.Name",  "trn{0}" );
                AddIfMissing("Deliver.Table.Name",  "{0}" );
				AddIfMissing("Publish.Table.Name",  "{0}" );
                AddIfMissing("Share.Table.Name",  "{0}" );
                AddIfMissing("General.Table.Description",  "Data from the table {0}" );
				AddIfMissing("Stage.Table.Description",  "The staging table for {0}" );
                AddIfMissing("Validate.Table.Description",  "The validation table for {0}" );
                AddIfMissing("Transform.Table.Description",  "The transform table for {0}" );
                AddIfMissing("Deliver.Table.Description",  "The delivered table for {0}" );
				AddIfMissing("Publish.Table.Description",  "The published data for {0}" );
                AddIfMissing("Share.Table.Description",  "Data from the table {0}" );
                AddIfMissing("Table.RejectName",  "Reject{0}" );
                AddIfMissing("Table.ProfileName",  "Profile{0}" );
                AddIfMissing("General.Datalink.Name",  "Data load for {0}" );
                AddIfMissing("Stage.Datalink.Name",  "Staging load for {0}" );
                AddIfMissing("Validate.Datalink.Name",  "Validation load for {0}" );
                AddIfMissing("Transform.Datalink.Name",  "Transform load for {0}" );
                AddIfMissing("Deliver.Datalink.Name",  "Deliver load for {0}" );
                AddIfMissing("Publish.Datalink.Name",  "Publish load for {0}" );
                AddIfMissing("Share.Datalink.Name",  "Data for {0}" );
                AddIfMissing("CreateDate.Column.Name",  "CreateDate" );
                AddIfMissing("CreateDate.Column.Logical",  "CreateDate" );
                AddIfMissing("CreateDate.Column.Description",  "The date and time the record first created." );
                AddIfMissing("UpdateDate.Column.Name",  "UpdateDate" );
                AddIfMissing("UpdateDate.Column.Logical",  "UpdateDate" );
                AddIfMissing("UpdateDate.Column.Description",  "The date and time the record last updated." );
                AddIfMissing("CreateAuditKey.Column.Name",  "CreateAuditKey" );
                AddIfMissing("CreateAuditKey.Column.Logical",  "CreateAuditKey" );
                AddIfMissing("CreateAuditKey.Column.Description",  "Link to the audit key that created the record." );
                AddIfMissing("UpdateAuditKey.Column.Name",  "UpdateAuditKey" );
                AddIfMissing("UpdateAuditKey.Column.Logical",  "UpdateAuditKey" );
                AddIfMissing("UpdateAuditKey.Column.Description",  "Link to the audit key that updated the record." );
                AddIfMissing("AutoIncrement.Column.Name",  "{0}Sk" );
                AddIfMissing("AutoIncrement.Column.Logical",  "{0}Sk" );
                AddIfMissing("AutoIncrement.Column.Description",  "The surrogate key created for the table {0}." );
                AddIfMissing("ValidFromDate.Column.Name",  "ValidFromDate" );
                AddIfMissing("ValidFromDate.Column.Logical",  "ValidFromDate" );
                AddIfMissing("ValidFromDate.Column.Description",  "The date and time the record becomes valid." );
                AddIfMissing("ValidToDate.Column.Name",  "ValidToDate" );
                AddIfMissing("ValidToDate.Column.Logical",  "ValidToDate" );
                AddIfMissing("ValidToDate.Column.Description",  "The date and time the record becomes invalid." );
                AddIfMissing("IsCurrentField.Column.Name",  "IsCurrent" );
                AddIfMissing("IsCurrentField.Column.Logical",  "IsCurrent" );
				AddIfMissing("IsCurrentField.Column.Description",  "True/False - Is the current record within the valid range?" );
                AddIfMissing("Version.Column.Name",  "Version" );
                AddIfMissing("Version.Column.Logical",  "Version" );
                AddIfMissing("Version.Column.Description",  "Version number of preserved records." );
                AddIfMissing("SourceSurrogateKey.Column.Name",  "SourceSk" );
                AddIfMissing("SourceSurrogateKey.Column.Logical",  "SourceSk" );
                AddIfMissing("SourceSurrogateKey.Column.Description",  "The surrogate key from the source table." );
                AddIfMissing("ValidationStatus.Column.Name",  "ValidationStatus" );
                AddIfMissing("ValidationStatus.Column.Logical",  "ValidationStatus" );
                AddIfMissing("ValidationStatus.Column.Description",  "Indicates if the record has passed validation tests." );
        }

        private void AddIfMissing(string name, string value)
        {
            if (!ContainsKey(name))
            {
                Add(name, value);
            }
        }

        private bool _defaultLoaded;
        
        public string ApplyNamingStandard(string name, string param1)
        {
            if (!_defaultLoaded)
            {
                LoadDefault();
                _defaultLoaded = true;
            }
            
            var namingStandard = this[name];
            if (namingStandard != null)
            {
                return namingStandard.Replace("{0}", param1);
            }

            throw new Exception($"The naming standard for the name \"{name}\" with parameter \"{param1}\" could not be found.");
        }
    }}