using Polars.Native;

namespace Polars.CSharp;
/// <summary>
/// Enums of JoinTypes
/// </summary>
public enum JoinType
{
    /// <summary>
    /// Inner Join
    /// </summary>
    Inner,
    /// <summary>
    /// Left Join
    /// </summary>
    Left,
    /// <summary>
    /// Full Join
    /// </summary>
    Outer,
    /// <summary>
    /// Cross Join
    /// </summary>
    Cross,
    /// <summary>
    /// Semi Join
    /// </summary>
    Semi,
    /// <summary>
    /// Anti Join
    /// </summary>
    Anti
}
/// <summary>
/// Specifies the aggregation function for pivot operations.
/// </summary>
public enum PivotAgg
{
    /// <summary>
    /// Selects the first value encountered in the group.
    /// </summary>
    First,

    /// <summary>
    /// Computes the sum of the values in the group.
    /// </summary>
    Sum, 

    /// <summary>
    /// Finds the minimum value in the group.
    /// </summary>
    Min, 

    /// <summary>
    /// Finds the maximum value in the group.
    /// </summary>
    Max, 

    /// <summary>
    /// Computes the arithmetic mean (average) of the values in the group.
    /// </summary>
    Mean, 

    /// <summary>
    /// Computes the median of the values in the group.
    /// </summary>
    Median, 

    /// <summary>
    /// Counts the number of non-null values in the group.
    /// </summary>
    Count, 

    /// <summary>
    /// Computes the length of the group (number of rows).
    /// </summary>
    Len, 

    /// <summary>
    /// Selects the last value encountered in the group.
    /// </summary>
    Last
}

internal static class EnumExtensions
{
    //
    public static PlJoinType ToNative(this JoinType type) => type switch
    {
        JoinType.Inner => PlJoinType.Inner,
        JoinType.Left => PlJoinType.Left,
        JoinType.Outer => PlJoinType.Outer,
        JoinType.Cross => PlJoinType.Cross,
        JoinType.Semi => PlJoinType.Semi,
        JoinType.Anti => PlJoinType.Anti,
        _ => PlJoinType.Inner
    };

    //
    public static PlPivotAgg ToNative(this PivotAgg agg) => agg switch
    {
        PivotAgg.First => PlPivotAgg.First,
        PivotAgg.Sum => PlPivotAgg.Sum,
        PivotAgg.Min => PlPivotAgg.Min,
        PivotAgg.Max => PlPivotAgg.Max,
        PivotAgg.Mean => PlPivotAgg.Mean,
        PivotAgg.Median => PlPivotAgg.Median,
        PivotAgg.Count => PlPivotAgg.Count,
        PivotAgg.Len => PlPivotAgg.Len,
        PivotAgg.Last => PlPivotAgg.Last,
        _ => PlPivotAgg.First
    };
}