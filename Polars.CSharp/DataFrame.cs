using Polars.Native; // 引用底层绑定
using Apache.Arrow;
namespace Polars.CSharp;

/// <summary>
/// 表示一个急切执行 (Eager) 的 DataFrame。
/// 数据驻留在内存中。
/// </summary>
public class DataFrame : IDisposable
{
    internal DataFrameHandle Handle { get; }

    internal DataFrame(DataFrameHandle handle)
    {
        Handle = handle;
    }
    // ==========================================
    // Static IO Read
    // ==========================================
    /// <summary>
    /// Read CSV File
    /// </summary>
    /// <param name="path"></param>
    /// <param name="tryParseDates"></param>
    /// <returns></returns>
    public static DataFrame ReadCsv(string path, bool tryParseDates = true)
    {
        //
        return new DataFrame(PolarsWrapper.ReadCsv(path, tryParseDates));
    }
    /// <summary>
    /// Read Parquet File
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static DataFrame ReadParquet(string path)
    {
        //
        return new DataFrame(PolarsWrapper.ReadParquet(path));
    }
    /// <summary>
    /// Read JSON File
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static DataFrame ReadJson(string path)
    {
        //
        return new DataFrame(PolarsWrapper.ReadJson(path));
    }
    /// <summary>
    /// Read IPC File
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static DataFrame ReadIpc(string path)
    {
        //
        return new DataFrame(PolarsWrapper.ReadIpc(path));
    }

    /// <summary>
    /// Create DataFrame from Arrow RecordBatch
    /// </summary>
    public static DataFrame FromArrow(RecordBatch batch)
    {
        //
        return new DataFrame(PolarsWrapper.FromArrow(batch));
    }

    // ==========================================
    // Properties
    // ==========================================
    /// <summary>
    /// Return DataFrame Height
    /// </summary>
    public long Height => PolarsWrapper.DataFrameHeight(Handle); //
    /// <summary>
    /// Return DataFrame Width
    /// </summary>
    public long Width => PolarsWrapper.DataFrameWidth(Handle);   //
    /// <summary>
    /// Return DataFrame Columns' Name
    /// </summary>
    public string[] Columns => PolarsWrapper.GetColumnNames(Handle); //
    /// <summary>
    /// Output DataFrame structure to string
    /// </summary>
    /// <returns></returns>
    public override string ToString()
    {
        return $"DataFrame: {Height}x{Width} [{string.Join(", ", Columns)}]";
    }

    // ==========================================
    // DataFrame Operations
    // ==========================================
    /// <summary>
    /// Select columns
    /// </summary>
    /// <param name="exprs"></param>
    /// <returns></returns>
    public DataFrame Select(params Expr[] exprs)
    {
        // 必须 Clone Handle，因为 Wrapper 会消耗它们
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        
        //
        return new DataFrame(PolarsWrapper.Select(Handle, handles));
    }
    /// <summary>
    /// Filter rows based on a boolean expression. 
    /// </summary>
    /// <param name="expr"></param>
    /// <returns></returns>
    public DataFrame Filter(Expr expr)
    {
        var h = PolarsWrapper.CloneExpr(expr.Handle);
        //
        return new DataFrame(PolarsWrapper.Filter(Handle, h));
    }
    /// <summary>
    /// Filter rows based on a boolean expression. 
    /// </summary>
    /// <param name="exprs"></param>
    /// <returns></returns>
    public DataFrame WithColumns(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        //
        return new DataFrame(PolarsWrapper.WithColumns(Handle, handles));
    }
    /// <summary>
    /// Sort (Order By) the DataFrame.
    /// </summary>
    /// <param name="by"></param>
    /// <param name="descending"></param>
    /// <returns></returns>
    public DataFrame Sort(Expr by, bool descending = false)
    {
        var h = PolarsWrapper.CloneExpr(by.Handle);
        //
        return new DataFrame(PolarsWrapper.Sort(Handle, h, descending));
    }
    /// <summary>
    /// Return head lines from a DataFrame
    /// </summary>
    /// <param name="n"></param>
    /// <returns></returns>
    public DataFrame Head(int n = 5)
    {
        //
        return new DataFrame(PolarsWrapper.Head(Handle, (uint)n));
    }
    /// <summary>
    /// Explode a list or structure in a Column
    /// </summary>
    /// <param name="exprs"></param>
    /// <returns></returns>
    public DataFrame Explode(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        //
        return new DataFrame(PolarsWrapper.Explode(Handle, handles));
    }

    // ==========================================
    // Combining DataFrames
    // ==========================================
    /// <summary>
    /// Join with another DataFrame
    /// </summary>
    /// <param name="other"></param>
    /// <param name="leftOn"></param>
    /// <param name="rightOn"></param>
    /// <param name="how"></param>
    /// <returns></returns>
    public DataFrame Join(DataFrame other, Expr leftOn, Expr rightOn, JoinType how = JoinType.Inner)
    {
        var lOn = new[] { PolarsWrapper.CloneExpr(leftOn.Handle) };
        var rOn = new[] { PolarsWrapper.CloneExpr(rightOn.Handle) };
        
        //
        return new DataFrame(PolarsWrapper.Join(
            this.Handle, 
            other.Handle, 
            lOn, 
            rOn, 
            how.ToNative()
        ));
    }
    /// <summary>
    /// Concatenate multiple DataFrames vertically.
    /// </summary>
    /// <param name="dfs"></param>
    /// <returns></returns>
    public static DataFrame Concat(IEnumerable<DataFrame> dfs)
    {
        // Concat 比较特殊，Native Binding 也是 TransferOwnership
        // 如果我们希望 C# 层的原 df 还能用，这里必须 clone
        var handles = dfs.Select(d => PolarsWrapper.CloneDataFrame(d.Handle)).ToArray();
        
        //
        return new DataFrame(PolarsWrapper.Concat(handles));
    }

    // ==========================================
    // GroupBy
    // ==========================================
    /// <summary>
    /// Group by keys and apply aggregations.
    /// </summary>
    /// <param name="by"></param>
    /// <returns></returns>
    public GroupByBuilder GroupBy(params Expr[] by)
    {
        // 返回一个构建器，不立即执行
        return new GroupByBuilder(this, by);
    }

    // ==========================================
    // Pivot / Unpivot
    // ==========================================
    /// <summary>
    /// Pivot the DataFrame from long to wide format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="columns"></param>
    /// <param name="values"></param>
    /// <param name="agg"></param>
    /// <returns></returns>
    public DataFrame Pivot(string[] index, string[] columns, string[] values, PivotAgg agg = PivotAgg.First)
    {
        //
        return new DataFrame(PolarsWrapper.Pivot(Handle, index, columns, values, agg.ToNative()));
    }
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public DataFrame Unpivot(string[] index, string[] on, string variableName = "variable", string valueName = "value")
    {
        //
        return new DataFrame(PolarsWrapper.Unpivot(Handle, index, on, variableName, valueName));
    }
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public DataFrame Melt(string[] index, string[] on, string variableName = "variable", string valueName = "value") 
        => Unpivot(index, on, variableName, valueName);

    // ==========================================
    // IO Write
    // ==========================================
    /// <summary>
    /// Write DataFrame to CSV File
    /// </summary>
    /// <param name="path"></param>
    public void WriteCsv(string path)
    {
        //
        PolarsWrapper.WriteCsv(Handle, path);
    }
    /// <summary>
    /// Write DataFrame to Parquet File
    /// </summary>
    /// <param name="path"></param>
    public void WriteParquet(string path)
    {
        //
        PolarsWrapper.WriteParquet(Handle, path);
    }

    // ==========================================
    // Scalar Access & Interop
    // ==========================================
    /// <summary>
    /// Get Scalar Int from DataFrame
    /// </summary>
    /// <param name="colName"></param>
    /// <param name="row"></param>
    /// <returns></returns>
    public long? GetInt(string colName, int row) 
        => PolarsWrapper.GetInt(Handle, colName, row); //
    /// <summary>
    /// Get Scalar Double from DataFrame
    /// </summary>
    /// <param name="colName"></param>
    /// <param name="row"></param>
    /// <returns></returns>
    public double? GetDouble(string colName, int row) 
        => PolarsWrapper.GetDouble(Handle, colName, row); //
    /// <summary>
    /// Get Scalar String from DataFrame
    /// </summary>
    /// <param name="colName"></param>
    /// <param name="row"></param>
    /// <returns></returns>
    public string? GetString(string colName, int row) 
        => PolarsWrapper.GetString(Handle, colName, row); //
    /// <summary>
    /// Transfer a RecordBatch to Arrow
    /// </summary>
    /// <returns></returns>
    public RecordBatch ToArrow()
    {
        //
        return PolarsWrapper.Collect(Handle);
    }
    /// <summary>
    /// Clone a Expr
    /// </summary>
    /// <returns></returns>
    public DataFrame Clone()
    {
        //
        return new DataFrame(PolarsWrapper.CloneDataFrame(Handle));
    }
    /// <summary>
    /// Delete a Expr
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
    }
}