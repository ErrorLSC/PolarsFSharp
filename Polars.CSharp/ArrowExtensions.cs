using Apache.Arrow;
using Apache.Arrow.Types;

namespace Polars.CSharp;
/// <summary>
///  Deal types of Arrow and C#
/// </summary>
public static class ArrowExtensions
{
    /// <summary>
    /// Read Arrow String, including StringViewArray
    /// </summary>
    /// <param name="array"></param>
    /// <param name="index"></param>
    /// <returns></returns>
    /// <exception cref="NotSupportedException"></exception>
    public static string? GetStringValue(this IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;

        switch (array)
        {
            // 标准 UTF8
            case StringArray sa:
                return sa.GetString(index);

            // Large UTF8 (Polars 旧版)
            case LargeStringArray lsa:
                return lsa.GetString(index);

            // StringView (Polars 0.50+ 默认)

            case StringViewArray sva:
                return sva.GetString(index);

            default:
                throw new NotSupportedException($"Unsupported string array type: {array.GetType().Name}");
        }
    }
    // ==========================================
    // 2. FormatValue
    // ==========================================
    /// <summary>
    /// Deal with Other Formats
    /// </summary>
    /// <param name="array"></param>
    /// <param name="index"></param>
    /// <returns></returns>
    public static string FormatValue(this IArrowArray array, int index)
    {
        if (array.IsNull(index)) return "null";

        return array switch
        {
            // 基础数值
            Int8Array arr   => arr.GetValue(index).ToString()!,
            Int16Array arr  => arr.GetValue(index).ToString()!,
            Int32Array arr  => arr.GetValue(index).ToString()!,
            Int64Array arr  => arr.GetValue(index).ToString()!,
            UInt8Array arr  => arr.GetValue(index).ToString()!,
            UInt16Array arr => arr.GetValue(index).ToString()!,
            UInt32Array arr => arr.GetValue(index).ToString()!,
            UInt64Array arr => arr.GetValue(index).ToString()!,
            FloatArray arr  => arr.GetValue(index).ToString()!,
            DoubleArray arr => arr.GetValue(index).ToString()!,

            // 字符串 (复用上面的 GetStringValue)
            StringArray sa      => $"\"{sa.GetStringValue(index)}\"",
            LargeStringArray lsa => $"\"{lsa.GetStringValue(index)}\"",
            StringViewArray sva  => $"\"{sva.GetStringValue(index)}\"",

            // 布尔
            BooleanArray arr => arr.GetValue(index).ToString()!.ToLower(),

            // Binary
            BinaryArray arr      => FormatBinary(arr.GetBytes(index)),
            LargeBinaryArray arr => FormatBinary(arr.GetBytes(index)),

            // 时间类型
            Date32Array arr => FormatDate32(arr, index),
            TimestampArray arr => FormatTimestamp(arr, index),
            Time32Array arr => FormatTime32(arr, index),
            Time64Array arr => FormatTime64(arr, index),
            DurationArray arr => FormatDuration(arr, index),

            // 嵌套类型
            ListArray arr      => FormatList(arr, index),
            LargeListArray arr => FormatLargeList(arr, index),
            StructArray arr => FormatStruct(arr, index),

            _ => $"<{array.GetType().Name}>"
        };
    }

    // --- Helpers ---
    /// <summary>
    /// Deal with Values
    /// </summary>
    /// <param name="array"></param>
    /// <param name="index"></param>
    /// <returns></returns>
    public static long? GetInt64Value(this IArrowArray array, int index)
    {
        if (array.IsNull(index)) return null;
        return array switch
        {
            // Signed Integes
            Int8Array  i8  => i8.GetValue(index),   // Polars Month/Day/Weekday is Int8
            Int16Array i16 => i16.GetValue(index),
            Int32Array i32 => i32.GetValue(index),
            Int64Array i64 => i64.GetValue(index),
            
            // Unsigned Integers (注意 UInt64 转 long 可能溢出为负数，但在常规数值处理中通常够用)
            UInt8Array  u8  => u8.GetValue(index),
            UInt16Array u16 => u16.GetValue(index),
            UInt32Array u32 => u32.GetValue(index),
            UInt64Array u64 => (long?)u64.GetValue(index),
            _ => null
        };
    }

    private static string FormatBinary(ReadOnlySpan<byte> bytes)
    {
        string hex = BitConverter.ToString(bytes.ToArray()).Replace("-", "").ToLower();
        return hex.Length > 20 ? $"x'{hex.Substring(0, 20)}...'" : $"x'{hex}'";
    }

    private static string FormatDate32(Date32Array arr, int index)
    {
        int days = arr.GetValue(index) ?? 0;
        return new DateTime(1970, 1, 1).AddDays(days).ToString("yyyy-MM-dd");
    }

    private static string FormatTimestamp(TimestampArray arr, int index)
    {
        long v = arr.GetValue(index) ?? 0;
        var unit = (arr.Data.DataType as TimestampType)?.Unit;
        long ticks = unit switch {
            TimeUnit.Nanosecond => v / 100L, TimeUnit.Microsecond => v * 10L,
            TimeUnit.Millisecond => v * 10000L, TimeUnit.Second => v * 10000000L, _ => v
        };
        try { return DateTime.UnixEpoch.AddTicks(ticks).ToString("yyyy-MM-dd HH:mm:ss.ffffff"); }
        catch { return v.ToString(); }
    }

    private static string FormatTime32(Time32Array arr, int index)
    {
        int v = arr.GetValue(index) ?? 0;
        var unit = (arr.Data.DataType as Time32Type)?.Unit;
        var span = unit switch { TimeUnit.Millisecond => TimeSpan.FromMilliseconds(v), _ => TimeSpan.FromSeconds(v) };
        return span.ToString();
    }

    private static string FormatTime64(Time64Array arr, int index)
    {
        long v = arr.GetValue(index) ?? 0;
        var unit = (arr.Data.DataType as Time64Type)?.Unit;
        long ticks = unit switch { TimeUnit.Nanosecond => v / 100L, _ => v * 10L };
        return TimeSpan.FromTicks(ticks).ToString();
    }

    private static string FormatDuration(DurationArray arr, int index)
    {
        long v = arr.GetValue(index) ?? 0;
        var unit = (arr.Data.DataType as DurationType)?.Unit;
        string suffix = unit switch {
            TimeUnit.Nanosecond => "ns", TimeUnit.Microsecond => "us",
            TimeUnit.Millisecond => "ms", TimeUnit.Second => "s", _ => ""
        };
        return $"{v}{suffix}";
    }

    private static string FormatList(ListArray arr, int index)
    {
        int start = arr.ValueOffsets[index];
        int end = arr.ValueOffsets[index + 1];
        var items = Enumerable.Range(start, end - start).Select(i => arr.Values.FormatValue(i));
        return $"[{string.Join(", ", items)}]";
    }

    private static string FormatLargeList(LargeListArray arr, int index)
    {
        int start = (int)arr.ValueOffsets[index];
        int end = (int)arr.ValueOffsets[index + 1];
        var items = Enumerable.Range(start, end - start).Select(i => arr.Values.FormatValue(i));
        return $"[{string.Join(", ", items)}]";
    }

    private static string FormatStruct(StructArray arr, int index)
    {
        var structType = arr.Data.DataType as StructType;
        if (structType == null) return "{}";
        var fields = structType.Fields.Select((field, i) => 
            $"{field.Name}: {arr.Fields[i].FormatValue(index)}");
        return $"{{{string.Join(", ", fields)}}}";
    }
}