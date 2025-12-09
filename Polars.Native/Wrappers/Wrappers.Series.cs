using System;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C; // 需要引用 Apache.Arrow

namespace Polars.Native;

public static partial class PolarsWrapper
{
    private static byte[]? ToBytes(bool[]? bools)
    {
        if (bools == null) return null;
        var bytes = new byte[bools.Length];
        for (int i = 0; i < bools.Length; i++)
        {
            bytes[i] = bools[i] ? (byte)1 : (byte)0;
        }
        return bytes;
    }
    // --- Constructors ---

    public static SeriesHandle SeriesNew(string name, int[] data, bool[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_i32(name, data, ToBytes(validity), (UIntPtr)data.Length));
    public static SeriesHandle SeriesNew(string name, long[] data, bool[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_i64(name, data, ToBytes(validity), (UIntPtr)data.Length));

    public static SeriesHandle SeriesNew(string name, double[] data, bool[]? validity) => 
        ErrorHelper.Check(NativeBindings.pl_series_new_f64(name, data, ToBytes(validity), (UIntPtr)data.Length));
        
    public static SeriesHandle SeriesNew(string name, bool[] data, bool[]? validity)
    {
        // 数据和 Validity 都需要转成 byte[]
        var dataBytes = ToBytes(data)!; // data 不可能为 null
        var validBytes = ToBytes(validity);
        return ErrorHelper.Check(NativeBindings.pl_series_new_bool(name, dataBytes, validBytes, (UIntPtr)data.Length));
    }

    // 字符串 Marshalling 比较特殊：需要手动分配非托管内存
    public static SeriesHandle SeriesNew(string name, string?[] data)
    {
        var len = data.Length;
        var ptrs = new IntPtr[len];
        
        try
        {
            for(int i=0; i<len; i++) 
            {
                if (data[i] == null)
                    ptrs[i] = IntPtr.Zero; // Rust 端会识别为 None
                else
                    ptrs[i] = Marshal.StringToCoTaskMemUTF8(data[i]);
            }
            
            return ErrorHelper.Check(NativeBindings.pl_series_new_str(name, ptrs, (UIntPtr)len));
        }
        finally 
        {
            // 必须释放刚刚分配的非托管字符串内存
            foreach(var p in ptrs) 
            {
                if (p != IntPtr.Zero) Marshal.FreeCoTaskMem(p);
            }
        }
    }
    
    // --- Properties ---

    public static long SeriesLen(SeriesHandle h) => (long)NativeBindings.pl_series_len(h);
    
    public static string SeriesName(SeriesHandle h) 
    {
        var ptr = NativeBindings.pl_series_name(h);
        try {
            return Marshal.PtrToStringUTF8(ptr) ?? "";
        } finally {
            NativeBindings.pl_free_c_string(ptr);
        }
    }
    
    public static void SeriesRename(SeriesHandle h, string name) => NativeBindings.pl_series_rename(h, name);

    // --- DataFrame Conversion ---
    public static DataFrameHandle SeriesToFrame(SeriesHandle h) 
    {
        return ErrorHelper.Check(NativeBindings.pl_series_to_frame(h));
    }
    
    // --- Arrow Integration ---

    public static unsafe IArrowArray SeriesToArrow(SeriesHandle h)
    {
        // 1. 获取 Rust Context
        // 注意：ArrowArrayContextHandle 也是 PolarsHandle，可以被 ErrorHelper 检查（如果需要的话）
        // 这里 NativeBindings 直接返回句柄，通常不需要 Check，除非 Rust 可能返回空指针
        using var contextHandle = NativeBindings.pl_series_to_arrow(h);
        
        // 2. 准备 C Data Interface 结构体
        var cArray = new CArrowArray();
        var cSchema = new CArrowSchema();
        
        // 3. 导出 (填充结构体)
        NativeBindings.pl_arrow_array_export(contextHandle, &cArray);
        NativeBindings.pl_arrow_schema_export(contextHandle, &cSchema);
        
        // 4. 导入 (两步走)
        try
        {
            // [修复] 第一步：先从 CSchema 导入字段定义 (Field)，获取 DataType
            var importedField = CArrowSchemaImporter.ImportField(&cSchema);
            
            // [修复] 第二步：使用 DataType 导入 Array
            // 这样编译器就能匹配到 ImportArray(CArrowArray*, IArrowType)
            var array = CArrowArrayImporter.ImportArray(&cArray, importedField.DataType);
            
            return array;
        }
        finally
        {
            // 注意：CArrowArrayImporter 会接管 cArray 和 cSchema 指向的内部资源 (release 回调)
            // 所以我们不需要手动释放 cArray/cSchema 结构体本身的内容
            // 只需要释放 Rust 的 Context 壳子 (由 using contextHandle 自动完成)
        }
    }
}