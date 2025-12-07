using Polars.Native;

namespace Polars.CSharp;
/// <summary>
/// Builder for LazyGroupByAggs
/// </summary>
public class LazyGroupByBuilder
{
    private readonly LazyFrame _lf;
    private readonly Expr[] _by;

    internal LazyGroupByBuilder(LazyFrame lf, Expr[] by)
    {
        _lf = lf;
        _by = by;
    }
    /// <summary>
    /// Aggregate with specified expressions
    /// </summary>
    /// <param name="aggs"></param>
    /// <returns></returns>
    public LazyFrame Agg(params Expr[] aggs)
    {
        var byHandles = _by.Select(b => PolarsWrapper.CloneExpr(b.Handle)).ToArray();
        var aggHandles = aggs.Select(a => PolarsWrapper.CloneExpr(a.Handle)).ToArray();

        // LazyGroupByAgg 消耗 _lf.Handle
        var h = PolarsWrapper.LazyGroupByAgg(_lf.Handle, byHandles, aggHandles);
        return new LazyFrame(h);
    }
}