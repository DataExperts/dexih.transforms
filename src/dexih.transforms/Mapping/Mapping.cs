using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;

namespace dexih.transforms.Mapping
{
    public abstract class Mapping
    {
        public bool IgnoreRow = false;
        
        public virtual TransformNode Transform { get; } = null;

        /// <summary>
        /// Initializes the mapping
        /// </summary>
        /// <param name="table"></param>
        /// <param name="joinTable"></param>
        /// <param name="mappings"></param>
        public abstract void InitializeColumns(Table table, Table joinTable = null, Mappings mappings = null);
        
        /// <summary>
        /// Runs any initialize functions for mappings, such as pre-loading data.
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
        /// <param name="functionVariables"></param>
        /// <param name="row"></param>
        /// <param name="joinRow"></param>
        /// <param name="cancellationToken"></param>
        /// <returns>0 filters or joins match, -1 row less than joinRow, 1 row greater than joinRow--></returns>
        public abstract Task<bool> ProcessInputRowAsync(FunctionVariables functionVariables, object[] row, object[] joinRow = null, CancellationToken cancellationToken = default);
        
        
        /// <summary>
        /// Gets the mapping result, and updates the row.
        /// </summary>
        /// <param name="row">The output row to populate</param>
        public abstract void MapOutputRow(object[] row);

        public abstract object GetOutputValue(object[] row = null);

        public abstract string Description();

        /// <summary>
        /// Runs any aggregate functions, and populates the aggregate results.
        /// </summary>
        /// <param name="functionVariables"></param>
        /// <param name="row">The output row to populate</param>
        /// <param name="functionType"></param>
        /// <param name="cancellationToken"></param>
        public virtual Task<bool> ProcessResultRowAsync(FunctionVariables functionVariables, object[] row, EFunctionType functionType, CancellationToken cancellationToken)
        {
            return Task.FromResult(false);
        }
        
        /// <summary>
        /// Runs any aggregate and other functions required a fill in a filler row (used by the series transform).
        /// </summary>
        /// <param name="functionVariables"></param>
        /// <param name="row">The output row to populate</param>
        /// <param name="functionType"></param>
        /// <param name="cancellationToken"></param>
        public virtual Task<bool> ProcessFillerRow(FunctionVariables functionVariables, object[] row, EFunctionType functionType, CancellationToken cancellationToken)
        {
            return Task.FromResult(false);
        }

//        public void ProcessResultRow(object[] row, EFunctionType functionType, CancellationToken cancellationToken)
//        {
//            ProcessResultRow(new FunctionVariables(), row, functionType, cancellationToken);
//        }

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

        public abstract IEnumerable<SelectColumn> GetRequiredColumns();

        public virtual IEnumerable<TableColumn> GetRequiredReferenceColumns()
        {
            return new TableColumn[0];
        }
    }
}
