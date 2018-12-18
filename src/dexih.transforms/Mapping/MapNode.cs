using System;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms.Mapping
{
    public class MapChapter: Mapping
    {
       
        public MapChapter(TableColumn chapterColumn)
        {
            ChapterColumn = chapterColumn;
        }
        
        public readonly TableColumn ChapterColumn;

        public override void InitializeColumns(Table table, Table joinTable = null)
        {
        }

        public override void AddOutputColumns(Table table)
        {
        }

        public override Task<bool> ProcessInputRow(FunctionVariables functionVariables, object[] row, object[] joinRow = null)
        {
            return Task.FromResult(true);
        }

        public override void MapOutputRow(object[] data) 
        {
        }

        public override object GetInputValue(object[] row = null)
        {
            throw new NotSupportedException();
        }

        public override string Description()
        {
            return $"Chapter {ChapterColumn?.Name}";
        }

        public override void Reset(EFunctionType functionType)
        {
        }

    }
}