using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms.Mappings
{
    public abstract class Mapping
    {
        
        /// <summary>
        /// Initializes the mapping
        /// </summary>
        /// <param name="table"></param>
        /// <param name="secondTable"></param>
        public abstract void InitializeColumns(Table table, Table joinTable = null);
        
        /// <summary>
        /// Runs any initialize functions for mappings, such as preloading data.
        /// </summary>
        public virtual Task Open()
        {
            return Task.CompletedTask;
        }
        
        /// <summary>
        /// Add the mapping columns to the table.
        /// </summary>
        /// <param name="table"></param>
        public abstract void AddOutputColumns(Table table);

        /// <summary>
        /// Runs the mapping for the specified row
        /// </summary>
        /// <param name="row"></param>
        /// <param name="joinRow"></param>
        /// <param name="seriesValue"></param>
        /// <returns>0 filters or joins match, -1 row lessthan joinRow, 1 row greater than joinRow--></returns>
        public abstract Task<bool> ProcessInputRow(FunctionVariables functionVariables, object[] row, object[] joinRow = null);

        public Task<bool> ProcessInputRow(object[] row, object[] joinRow = null)
        {
            return ProcessInputRow(new FunctionVariables(), row, joinRow);
        }

        /// <summary>
        /// Gets the mapping result, and updates the row.
        /// </summary>
        /// <param name="row">The output row to populate</param>
        public abstract void MapOutputRow(object[] row);

        public abstract object GetInputValue(object[] row = null);

        public abstract string Description();

        /// <summary>
        /// Runs any aggregate functions, and populates the aggregate results.
        /// </summary>
        /// <param name="index">The row within the current group for aggregate functions.</param>
        /// <param name="row">The output row to populate</param>
        public virtual Task<bool> ProcessResultRow(FunctionVariables functionVariables, object[] row, EFunctionType functionType)
        {
            return Task.FromResult(false);
        }

        public Task<bool> ProcessResultRow(object[] row, EFunctionType functionType)
        {
            return ProcessResultRow(new FunctionVariables(), row, functionType);
        }

        public virtual void ProcessFillerRow(object[] row, object[] fillerRow, object seriesValue) {}

        /// <summary>
        /// Run a reset (if needed) of the mapping.
        /// </summary>
        public virtual void Reset(EFunctionType functionType)
        {
        }

        public int AddOutputColumn(Table table, TableColumn column)
        {
            var ordinal = table.GetOrdinal(column);
            if (ordinal < 0)
            {
                table.Columns.Add(column);
                ordinal = table.Columns.Count - 1;
            }

            return ordinal;
        }
        
    }
}
