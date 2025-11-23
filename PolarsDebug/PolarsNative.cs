using System;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;

namespace Polars.Native;

// 1. 定义智能指针 (SafeHandle)
// 这代表了一个在 Rust 内存里活着的 DataFrame
public class DataFrameHandle : SafeHandle
{
    public DataFrameHandle() : base(IntPtr.Zero, true) { }

    public override bool IsInvalid => handle == IntPtr.Zero;

    // 当 C# 对象被回收时，自动执行这个释放逻辑
    protected override bool ReleaseHandle()
    {
        NativeBindings.pl_free_dataframe(handle);
        return true;
    }
}

// 新增 Expr 的智能指针
// Expr 是一次性的 (consumed by operation)，所以我们可能不需要太复杂的释放逻辑
public class ExprHandle : SafeHandle
{
    public ExprHandle() : base(IntPtr.Zero, true) { }
    public override bool IsInvalid => handle == IntPtr.Zero;
    protected override bool ReleaseHandle() { return true; } // 简化：Rust侧在 filter/gt 内部消耗了 Box，这里暂不处理
}

public class LazyFrameHandle : SafeHandle
{
    public LazyFrameHandle() : base(IntPtr.Zero, true) { }
    public override bool IsInvalid => handle == IntPtr.Zero;
    // LazyFrame 通常是一次性的（Collect 后就没了），
    // 或者是中间态，Rust 侧通过 Box::from_raw 消耗掉了，所以 C# 这里 Release 可以为空
    // 或者你可以实现一个 pl_free_lazyframe 来处理异常中断的情况。
    protected override bool ReleaseHandle() { return true; } 
}


// 2. 底层绑定
unsafe class NativeBindings
{
    const string LibName = "native_shim";

    [DllImport(LibName)]
    public static extern DataFrameHandle pl_read_csv([MarshalAs(UnmanagedType.LPUTF8Str)] string path,
    bool tryParseDates
    );

    [DllImport(LibName)]
    public static extern void pl_free_dataframe(IntPtr ptr);

    [DllImport(LibName)]
    public static extern void pl_to_arrow(DataFrameHandle handle, CArrowArray* arr, CArrowSchema* schema);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_col([MarshalAs(UnmanagedType.LPUTF8Str)] string name);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_lit_i32(int val);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_gt(ExprHandle left, ExprHandle right);

    [DllImport(LibName)]
    public static extern DataFrameHandle pl_filter(DataFrameHandle df, ExprHandle expr);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_lit_str([MarshalAs(UnmanagedType.LPUTF8Str)] string val);

    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_lit_f64(double val);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_eq(ExprHandle left, ExprHandle right);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_mul(ExprHandle left, ExprHandle right);

    [DllImport(LibName)]
    public static extern ExprHandle pl_expr_alias(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string name);

    [DllImport(LibName)]
    public static extern DataFrameHandle pl_select(DataFrameHandle df, IntPtr[] exprs, UIntPtr len);

    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_sum(ExprHandle expr);

    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_mean(ExprHandle expr);

    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_max(ExprHandle expr);

    [DllImport(LibName)] 
    public static extern ExprHandle pl_expr_dt_year(ExprHandle expr);

    [DllImport(LibName)]
    public static extern DataFrameHandle pl_groupby_agg(
        DataFrameHandle df, 
        IntPtr[] byExprs, UIntPtr byLen,
        IntPtr[] aggExprs, UIntPtr aggLen
    );

    // Join 签名
    [DllImport(LibName)]
    public static extern DataFrameHandle pl_join(
        DataFrameHandle left,
        DataFrameHandle right,
        IntPtr[] leftOn, UIntPtr leftLen,
        IntPtr[] rightOn, UIntPtr rightLen,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string how
    );
    // Parquet
    [DllImport(LibName)] 
    public static extern void pl_write_csv(DataFrameHandle df, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);
    [DllImport(LibName)] 
    public static extern void pl_write_parquet(DataFrameHandle df, [MarshalAs(UnmanagedType.LPUTF8Str)] string path);
    [DllImport(LibName)] 
    public static extern DataFrameHandle pl_read_parquet([MarshalAs(UnmanagedType.LPUTF8Str)] string path);

    // Lazy
    [DllImport(LibName)] 
    public static extern LazyFrameHandle pl_scan_csv([MarshalAs(UnmanagedType.LPUTF8Str)] string path,
    bool tryParseDates
    );

    [DllImport(LibName)] 
    public static extern LazyFrameHandle pl_scan_parquet([MarshalAs(UnmanagedType.LPUTF8Str)] string path);
    
    [DllImport(LibName)] 
    public static extern LazyFrameHandle pl_lazy_filter(LazyFrameHandle lf, ExprHandle expr);
    [DllImport(LibName)] 
    public static extern LazyFrameHandle pl_lazy_select(LazyFrameHandle lf, IntPtr[] exprs, UIntPtr len);
    [DllImport(LibName)] 
    public static extern LazyFrameHandle pl_lazy_sort(LazyFrameHandle lf, ExprHandle expr, bool desc);
    [DllImport(LibName)]
     public static extern DataFrameHandle pl_lazy_collect(LazyFrameHandle lf);

    [DllImport(LibName)] public static extern LazyFrameHandle pl_lazy_limit(LazyFrameHandle lf, uint n);
    [DllImport(LibName)] public static extern LazyFrameHandle pl_lazy_with_columns(LazyFrameHandle lf, IntPtr[] exprs, UIntPtr len);
    [DllImport(LibName)] public static extern ExprHandle pl_expr_str_contains(ExprHandle expr, [MarshalAs(UnmanagedType.LPUTF8Str)] string pat);

    [DllImport(LibName)] public static extern IntPtr pl_get_last_error();
    [DllImport(LibName)] public static extern void pl_free_error_msg(IntPtr ptr);

}

// 3. 高层封装
public static class PolarsWrapper
{
    // 检查 Handle 是否有效，无效则抛出 Rust 异常
    private static void CheckError(SafeHandle handle)
    {
        if (!handle.IsInvalid) return; // 有效，直接返回

        // 获取错误消息
        IntPtr msgPtr = NativeBindings.pl_get_last_error();
        if (msgPtr == IntPtr.Zero)
        {
            throw new Exception("Rust operation failed, but no error message was available.");
        }

        try
        {
            string msg = Marshal.PtrToStringUTF8(msgPtr) ?? "Unknown Rust Error";
            // 抛出带信息的异常！
            throw new Exception($"[Rust Error] {msg}");
        }
        finally
        {
            // 释放 Rust 侧的字符串内存
            NativeBindings.pl_free_error_msg(msgPtr);
        }
    }

    // 注意：现在返回的是 Handle，不是数据！
    public static DataFrameHandle ReadCsv(string path, bool tryParseDates)
    {
        if (!File.Exists(path)) throw new FileNotFoundException($"CSV not found: {path}");
        var h = NativeBindings.pl_read_csv(path, tryParseDates);
        CheckError(h);
        return h;
    }

    // 新增：显式把 Handle 转成 Arrow 数据
    public static unsafe RecordBatch Collect(DataFrameHandle handle)
    {
        var array = CArrowArray.Create();
        var schema = CArrowSchema.Create();
        try
        {
            NativeBindings.pl_to_arrow(handle, array, schema);
            var managedSchema = CArrowSchemaImporter.ImportSchema(schema);
            return CArrowArrayImporter.ImportRecordBatch(array, managedSchema);
        }
        finally
        {
            CArrowArray.Free(array);
            CArrowSchema.Free(schema);
        }
    }
    public static DataFrameHandle ReadParquet(string path)
    {
         if (!File.Exists(path)) throw new FileNotFoundException($"Parquet not found: {path}");
         var h = NativeBindings.pl_read_parquet(path);
         if (h.IsInvalid) throw new Exception("Failed to load Parquet");
         return h;
    }
    public static void WriteCsv(DataFrameHandle df, string path) => NativeBindings.pl_write_csv(df, path);
    public static void WriteParquet(DataFrameHandle df, string path) => NativeBindings.pl_write_parquet(df, path);

    public static ExprHandle Col(string name) => NativeBindings.pl_expr_col(name);
    public static ExprHandle Lit(int val) => NativeBindings.pl_expr_lit_i32(val);
    public static ExprHandle Lit(string val) => NativeBindings.pl_expr_lit_str(val);
    public static ExprHandle Lit(double val) => NativeBindings.pl_expr_lit_f64(val);
    public static ExprHandle Mul(ExprHandle left, ExprHandle right) => NativeBindings.pl_expr_mul(left, right);
    public static ExprHandle Alias(ExprHandle expr, string name) => NativeBindings.pl_expr_alias(expr, name);
    public static ExprHandle Gt(ExprHandle left, ExprHandle right) 
        => NativeBindings.pl_expr_gt(left, right);
    public static ExprHandle Eq(ExprHandle left, ExprHandle right) 
        => NativeBindings.pl_expr_eq(left, right);
    public static ExprHandle Sum(ExprHandle e) => NativeBindings.pl_expr_sum(e);
    public static ExprHandle Mean(ExprHandle e) => NativeBindings.pl_expr_mean(e);
    public static ExprHandle Max(ExprHandle e) => NativeBindings.pl_expr_max(e);
    public static DataFrameHandle Filter(DataFrameHandle df, ExprHandle expr)
    {
        var h = NativeBindings.pl_filter(df, expr);
        CheckError(h); // <--- 使用 CheckError
        return h;
    }

    public static DataFrameHandle Select(DataFrameHandle df, ExprHandle[] handles)
    {
        // 1. 提取所有 Handle 的原始指针 (IntPtr)
        // 这一步是为了构建传给 C 的指针数组
        IntPtr[] rawHandles = new IntPtr[handles.Length];
        for (int i = 0; i < handles.Length; i++)
        {
            // 这一步告诉 Handle："我要用这个指针，你在我用完之前别释放它"
            // 虽然我们的 ExprHandle 也没啥释放逻辑，但这是好习惯
            rawHandles[i] = handles[i].DangerousGetHandle();
            
            // 技巧：在 Rust 里我们用了 Box::from_raw，这意味着 Rust 接管了内存。
            // 所以 C# 这边的 Handle 实际上已经"失效"了。
            // 为了防止 C# GC 二次释放（虽然我们现在的 ReleaseHandle 是空的），
            // 标准做法是调用 SetHandleAsInvalid。这里为了简单暂不处理，因为 ReleaseHandle 是空的。
        }

        // 2. 调用 Rust
        var newHandle = NativeBindings.pl_select(df, rawHandles, (UIntPtr)handles.Length);
        
        if (newHandle.IsInvalid) throw new Exception("Select failed");
        return newHandle;
    }

    // GroupBy 封装
    public static DataFrameHandle GroupByAgg(DataFrameHandle df, ExprHandle[] by, ExprHandle[] agg)
    {
        // 转换两个数组
        IntPtr[] rawBy = System.Array.ConvertAll(by, h => h.DangerousGetHandle());
        IntPtr[] rawAgg = System.Array.ConvertAll(agg, h => h.DangerousGetHandle());

        var h = NativeBindings.pl_groupby_agg(
            df, 
            rawBy, (UIntPtr)rawBy.Length,
            rawAgg, (UIntPtr)rawAgg.Length
        );

        if (h.IsInvalid) throw new Exception("GroupBy failed");
        return h;
    }
    public static DataFrameHandle Join(
        DataFrameHandle left, 
        DataFrameHandle right, 
        ExprHandle[] leftOn, 
        ExprHandle[] rightOn, 
        string how)
    {
        // 转换两个 Expr 数组
        IntPtr[] rawLeftOn = System.Array.ConvertAll(leftOn, h => h.DangerousGetHandle());
        IntPtr[] rawRightOn = System.Array.ConvertAll(rightOn, h => h.DangerousGetHandle());

        var h = NativeBindings.pl_join(
            left, right,
            rawLeftOn, (UIntPtr)rawLeftOn.Length,
            rawRightOn, (UIntPtr)rawRightOn.Length,
            how
        );

        if (h.IsInvalid) throw new Exception("Join failed");
        return h;
    }
    // Lazy API
    public static LazyFrameHandle ScanCsv(string path, bool tryParseDates) 
    {
        var h = NativeBindings.pl_scan_csv(path,tryParseDates);
        CheckError(h);
        return h;
    } 
    public static LazyFrameHandle ScanParquet(string path) => NativeBindings.pl_scan_parquet(path);
    
    public static LazyFrameHandle LazyFilter(LazyFrameHandle lf, ExprHandle expr) => NativeBindings.pl_lazy_filter(lf, expr);
    
    public static LazyFrameHandle LazySelect(LazyFrameHandle lf, ExprHandle[] handles)
    {
        IntPtr[] raw = System.Array.ConvertAll(handles, h => h.DangerousGetHandle());
        return NativeBindings.pl_lazy_select(lf, raw, (UIntPtr)raw.Length);
    }

    public static LazyFrameHandle LazySort(LazyFrameHandle lf, ExprHandle expr, bool desc) => NativeBindings.pl_lazy_sort(lf, expr, desc);

    public static DataFrameHandle LazyCollect(LazyFrameHandle lf)
    {
        var h = NativeBindings.pl_lazy_collect(lf);
        if (h.IsInvalid) throw new Exception("Collect failed");
        return h;
    }

    public static LazyFrameHandle LazyLimit(LazyFrameHandle lf, uint n) => NativeBindings.pl_lazy_limit(lf, n);
    
    public static LazyFrameHandle LazyWithColumns(LazyFrameHandle lf, ExprHandle[] handles)
    {
        IntPtr[] raw = System.Array.ConvertAll(handles, h => h.DangerousGetHandle());
        return NativeBindings.pl_lazy_with_columns(lf, raw, (UIntPtr)raw.Length);
    }

    public static ExprHandle StrContains(ExprHandle expr, string pat) => NativeBindings.pl_expr_str_contains(expr, pat);

    // Temporal
    public static ExprHandle DtYear(ExprHandle expr) => NativeBindings.pl_expr_dt_year(expr);
}    