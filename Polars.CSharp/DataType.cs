using System;
using Polars.Native;

namespace Polars.CSharp;

/// <summary>
/// Represents a Polars data type. 
/// Wraps the underlying Rust DataType.
/// </summary>
public class DataType : IDisposable
{
    internal DataTypeHandle Handle { get; }

    internal DataType(DataTypeHandle handle)
    {
        Handle = handle;
    }
    /// <summary>
    /// Dispose the underlying DataTypeHandle.
    /// </summary>
    public void Dispose()
    {
        Handle.Dispose();
    }

    // ==========================================
    // Primitive Factories (Static Properties)
    // ==========================================
    
    // 每次调用都会创建一个新的 Handle，由 SafeHandle 负责释放
    /// <summary>
    /// Boolean Data Type
    /// </summary>
    public static DataType Boolean => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Boolean));
    /// <summary>
    /// Int8 Data Type
    /// </summary>
    public static DataType Int8 => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int8));
    /// <summary>
    /// Int16 Data Type
    /// </summary>
    public static DataType Int16 => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int16));
    /// <summary>
    /// Int32 Data Type
    /// </summary>
    public static DataType Int32 => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int32));
    /// <summary>
    /// Int64 Data Type
    /// </summary>
    public static DataType Int64 => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Int64));
    /// <summary>
    /// UInt8 Data Type
    /// </summary>
    public static DataType UInt8 => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt8));
    /// <summary>
    /// UInt16 Data Type
    /// </summary>
    public static DataType UInt16 => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt16));
    /// <summary>
    /// UInt32 Data Type
    /// </summary>
    public static DataType UInt32 => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt32));
    /// <summary>
    /// UInt64 Data Type
    /// </summary>
    public static DataType UInt64 => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.UInt64));
    /// <summary>
    /// Float32 Data Type
    /// </summary>
    public static DataType Float32 => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Float32));
    /// <summary>
    /// Float64 Data Type
    /// </summary>
    public static DataType Float64 => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Float64));
    /// <summary>
    /// String Data Type
    /// </summary>
    public static DataType String => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.String));
    /// <summary>
    /// Date Data Type
    /// </summary>
    public static DataType Date => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Date));
    /// <summary>
    /// Datetime Data Type
    /// </summary>
    public static DataType Datetime => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Datetime));
    /// <summary>
    /// Time Data Type
    /// </summary>
    public static DataType Time => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Time));
    /// <summary>
    /// Duration Data Type
    /// </summary>
    public static DataType Duration => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Duration));
    /// <summary>
    /// Binary Data Type
    /// </summary>    
    public static DataType Binary => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.Binary));
    /// <summary>
    /// Use the same data type as the input expression (Special type for UDFs).
    /// </summary>
    public static DataType SameAsInput => new DataType(PolarsWrapper.NewPrimitiveType((int)PlDataType.SameAsInput));

    // ==========================================
    // Complex Factories (Methods)
    // ==========================================

    /// <summary>
    /// Create a Decimal data type with specific precision and scale.
    /// </summary>
    public static DataType Decimal(int precision, int scale) 
        => new DataType(PolarsWrapper.NewDecimalType(precision, scale));

    /// <summary>
    /// Create a Categorical data type.
    /// </summary>
    public static DataType Categorical 
        => new DataType(PolarsWrapper.NewCategoricalType());
}