using System;
using Polars.Native;
using Apache.Arrow;

namespace Polars.CSharp;
/// <summary>
/// Represents a Polars Series.
/// </summary>
public class Series : IDisposable
{
    internal SeriesHandle Handle { get; }

    internal Series(SeriesHandle handle)
    {
        Handle = handle;
    }

    // ==========================================
    // Constructors
    // ==========================================
    /// <summary>
    /// Create a Series from an array of integers.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, int[] data, bool[]? validity = null)
    {
        Handle = PolarsWrapper.SeriesNew(name, data, validity);
    }
    /// <summary>
    /// Create a Series from an array of longs.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, long[] data, bool[]? validity = null)
    {
        Handle = PolarsWrapper.SeriesNew(name, data, validity);
    }
    /// <summary>
    /// Create a Series from an array of doubles.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, double[] data, bool[]? validity = null)
    {
        Handle = PolarsWrapper.SeriesNew(name, data, validity);
    }
    /// <summary>
    /// Create a Series from an array of booleans.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    /// <param name="validity"></param>
    public Series(string name, bool[] data, bool[]? validity = null)
    {
        Handle = PolarsWrapper.SeriesNew(name, data, validity);
    }
    /// <summary>
    /// Create a Series from an array of strings.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="data"></param>
    public Series(string name, string?[] data)
    {
        Handle = PolarsWrapper.SeriesNew(name, data);
    }

    // ==========================================
    // Properties
    // ==========================================
    /// <summary>
    /// Length of the Series.
    /// </summary>
    public long Length => PolarsWrapper.SeriesLen(Handle);
    /// <summary>
    /// Name of the Series.
    /// </summary>
    public string Name 
    {
        get => PolarsWrapper.SeriesName(Handle);
        set => PolarsWrapper.SeriesRename(Handle, value);
    }

    // ==========================================
    // Operations
    // ==========================================

    /// <summary>
    /// Cast the Series to a different DataType.
    /// </summary>
    public Series Cast(DataType dtype)
    {
        // SeriesCast 返回一个新的 Series Handle
        return new Series(PolarsWrapper.SeriesCast(Handle, dtype.Handle));
    }

    // ==========================================
    // Conversions (Arrow / DataFrame)
    // ==========================================

    /// <summary>
    /// Zero-copy convert to Apache Arrow Array.
    /// </summary>
    public IArrowArray ToArrow()
    {
        return PolarsWrapper.SeriesToArrow(Handle);
    }

    /// <summary>
    /// Convert this single Series into a DataFrame.
    /// </summary>
    public DataFrame ToFrame()
    {
        return new DataFrame(PolarsWrapper.SeriesToFrame(Handle));
    }
    /// <summary>
    /// Dispose the underlying SeriesHandle.
    /// </summary>
    public void Dispose()
    {
        Handle.Dispose();
    }
}