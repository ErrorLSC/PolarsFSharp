namespace PolarsFSharp

open System
open System.IO
open Apache.Arrow
open Apache.Arrow.Types
open Polars.Native

// ==========================================
// 1. 核心类型
// ==========================================
type DataFrame(handle: DataFrameHandle) =
    interface IDisposable with
        member _.Dispose() = handle.Dispose()
    
    member _.Handle = handle
    member this.ToArrow() = PolarsWrapper.Collect(handle)
    // 此处以后应修改为从rust拿数
    member this.Rows = 
        use batch = this.ToArrow()
        batch.Length
    // 此处以后应修改为从rust拿数
    member this.Columns = 
        use batch = this.ToArrow()
        batch.ColumnCount

// 新增：LazyFrame 类型
and LazyFrame(handle: LazyFrameHandle) =
    member _.Handle = handle
    
    // 只有 LazyFrame 才有 Collect 方法
    member this.Collect() = 
        let dfHandle = PolarsWrapper.LazyCollect(handle)
        new DataFrame(dfHandle)
// ==========================================
// Expr 类型封装
// ==========================================
type Expr(handle: ExprHandle) =
    member _.Handle = handle

    // 大于
    static member (.>) (lhs: Expr, rhs: Expr) =
        new Expr(PolarsWrapper.Gt(lhs.Handle, rhs.Handle))

    // 注意：F# 不允许重写标准的 =，所以我们用 .==
    static member (.==) (lhs: Expr, rhs: Expr) =
        new Expr(PolarsWrapper.Eq(lhs.Handle, rhs.Handle))

    // 新增：乘法 (.*)
    static member (.*) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Mul(lhs.Handle, rhs.Handle))
    
    // 新增：别名
    member this.Alias(name: string) = new Expr(PolarsWrapper.Alias(handle, name))
    // 新增聚合方法
    member this.Sum() = new Expr(PolarsWrapper.Sum(handle))
    member this.Mean() = new Expr(PolarsWrapper.Mean(handle))
    member this.Max() = new Expr(PolarsWrapper.Max(handle))
    // [新增] 字符串操作
    // 用法: col("name").StrContains("Bob")
    member this.StrContains(pattern: string) = new Expr(PolarsWrapper.StrContains(handle, pattern))

    member this.Dt = new DtOps(handle)

and DtOps(handle: ExprHandle) =
        member _.Year() = new Expr(PolarsWrapper.DtYear(handle))

// ==========================================
// 2. 模块
// ==========================================
module Polars =
    // Incoming Data
    let readCsv (path: string) (tryParseDates: bool option): DataFrame =
        let parseDates = defaultArg tryParseDates true
        let handle = PolarsWrapper.ReadCsv(path,parseDates)
        new DataFrame(handle)

    let readParquet (path: string) = new DataFrame(PolarsWrapper.ReadParquet(path))

    // Outgoing Data
    let writeCsv (path: string) (df: DataFrame) = 
        PolarsWrapper.WriteCsv(df.Handle, path)
        df // 返回 df 支持链式

    let writeParquet (path: string) (df: DataFrame) = 
        PolarsWrapper.WriteParquet(df.Handle, path)
        df
    // ============================================
    // 黑魔法区域：万能 lit
    // ============================================
    
    // 1. 定义一个“标记类型”，并在其上利用 ($) 运算符定义重载
    // 这是 F# 实现 Ad-hoc 多态的标准范式
    type LitMechanism = LitMechanism with
        // 针对 int 的重载
        static member ($) (LitMechanism, v: int) = new Expr(PolarsWrapper.Lit(v))
        // 针对 string 的重载
        static member ($) (LitMechanism, v: string) = new Expr(PolarsWrapper.Lit(v))
        // double
        static member ($) (LitMechanism, v: double) = new Expr(PolarsWrapper.Lit(v))

    // 2. 定义 inline 函数
    // 这里的语法含义是：
    // "在编译时，查找类型 ^T 或 LitMechanism 上定义的 ($) 运算符，并调用它"
    // 因为我们在 LitMechanism 上定义了针对 int 和 string 的 ($)，编译器就能准确找到了。
    let inline lit (value: ^T) : Expr = 
        ((^T or LitMechanism) : (static member ($) : LitMechanism * ^T -> Expr) (LitMechanism, value))

    let private formatValue (col: IArrowArray) (index: int) : string =
         if col.IsNull(index) then "(null)"
         else
            match col with
            | :? Int32Array as arr -> string (arr.GetValue(index))
            | :? Int64Array as arr -> string (arr.GetValue(index))
            | :? DoubleArray as arr -> string (arr.GetValue(index))
            | :? StringArray as arr -> arr.GetString(index)
            | :? LargeStringArray as arr -> arr.GetString(index)
            | :? StringViewArray as arr -> arr.GetString(index)
            | :? BooleanArray as arr -> string (arr.GetValue(index))
            | :? Date32Array as arr -> 
                // GetValue 返回的是 Nullable<int>
                let v = arr.GetValue(index)
                if v.HasValue then 
                    DateTime(1970, 1, 1).AddDays(float v.Value).ToString("yyyy-MM-dd")
                else "(null)"
            // [新增] 时间戳处理 (Polars 默认可能是 Microsecond)
            // Apache.Arrow 的 TimestampArray 比较复杂，它有 Unit 属性
            | :? TimestampArray as arr -> 
                let v = arr.GetValue(index) // Nullable<long>
                if v.HasValue then v.Value.ToString() else "(null)"

            | _ -> sprintf "[%s]" (col.GetType().Name)
            

    let show (df: DataFrame) : DataFrame =
        // 获取数据用于显示 (Use 确保用完释放 Arrow 内存)
        use batch = df.ToArrow()

        printfn "\n--- Polars Remote DataFrame (Visible Rows: %d) ---" batch.Length
        let fields = batch.Schema.FieldsList

        // 注意这里的缩进！
        for field in fields do
            // 循环内部必须比 for 缩进更多
            let col = batch.Column(field.Name)
            // 处理 Data.DataType 可能会有空引用的情况，加个简单的保护
            let typeName = if isNull col.Data then "Unknown" else col.Data.DataType.Name
            
            printfn "[Column: '%s' (%s)]" field.Name typeName
            
            let limit = Math.Min(batch.Length, 5)
            
            // 内层循环
            for i in 0 .. limit - 1 do
                // 这里的缩进必须比上面的 for 更多
                let valStr = formatValue col i
                printfn "  Row %d: %s" i valStr
            
            if batch.Length > 5 then printfn "  ..."
            printfn ""

        df // 返回 df，这一行要和 use batch 对齐

    // 制造积木的函数
    let col (name: string) = new Expr(PolarsWrapper.Col(name))
    // let lit (value: int) = new Expr(PolarsWrapper.Lit(value))
    // let litStr (value: string) = new Expr(PolarsWrapper.Lit(value))
    // [新增] 辅助链式调用的 alias
    let alias (name: string) (expr: Expr) = expr.Alias(name)
    // 通用筛选函数
    let filter (expr: Expr) (df: DataFrame) : DataFrame =
        let h = PolarsWrapper.Filter(df.Handle, expr.Handle)
        new DataFrame(h)
    // [新增] Select 函数
    // 接受 Expr list
    let select (exprs: Expr list) (df: DataFrame) : DataFrame =
        // 把 F# list 转成 C# 数组
        let handles = exprs |> List.map (fun e -> e.Handle) |> List.toArray
        let h = PolarsWrapper.Select(df.Handle, handles)
        new DataFrame(h)

    let sum (e: Expr) = e.Sum()
    let mean (e: Expr) = e.Mean()
    let max (e: Expr) = e.Max()

    // [新增] GroupBy
    // 接受两个列表：keys (分组列) 和 aggs (聚合表达式)
    let groupBy (keys: Expr list) (aggs: Expr list) (df: DataFrame) : DataFrame =
        let kHandles = keys |> List.map (fun e -> e.Handle) |> List.toArray
        let aHandles = aggs |> List.map (fun e -> e.Handle) |> List.toArray
        
        let h = PolarsWrapper.GroupByAgg(df.Handle, kHandles, aHandles)
        new DataFrame(h)

    // [新增] Join 函数
    // 用法: df |> Polars.join otherDf [col "id"] [col "user_id"] "left"
    let join (other: DataFrame) (leftOn: Expr list) (rightOn: Expr list) (how: string) (left: DataFrame) : DataFrame =
        let lHandles = leftOn |> List.map (fun e -> e.Handle) |> List.toArray
        let rHandles = rightOn |> List.map (fun e -> e.Handle) |> List.toArray
        
        let h = PolarsWrapper.Join(left.Handle, other.Handle, lHandles, rHandles, how)
        new DataFrame(h)

    // ==========================================
    // Lazy API
    // ==========================================
    
    let scanCsv (path: string) (tryParseDates: bool option) = 
        let parseDates = defaultArg tryParseDates true
        new LazyFrame(PolarsWrapper.ScanCsv(path, parseDates))

    let scanParquet (path: string) = new LazyFrame(PolarsWrapper.ScanParquet(path))

    // Lazy 版本的 filter
    let filterLazy (expr: Expr) (lf: LazyFrame) : LazyFrame =
        let h = PolarsWrapper.LazyFilter(lf.Handle, expr.Handle)
        new LazyFrame(h)

    // Lazy 版本的 select
    let selectLazy (exprs: Expr list) (lf: LazyFrame) : LazyFrame =
        let handles = exprs |> List.map (fun e -> e.Handle) |> List.toArray
        let h = PolarsWrapper.LazySelect(lf.Handle, handles)
        new LazyFrame(h)
        
    // Lazy 版本的 sort (orderBy)
    let orderBy (expr: Expr) (desc: bool) (lf: LazyFrame) : LazyFrame =
        let h = PolarsWrapper.LazySort(lf.Handle, expr.Handle, desc)
        new LazyFrame(h)

    // [新增] Limit
    let limit (n: uint) (lf: LazyFrame) : LazyFrame =
        let h = PolarsWrapper.LazyLimit(lf.Handle, n)
        new LazyFrame(h)

    // [新增] WithColumn (添加单列)
    // 用法: df |> withColumn (col "age" .* lit 2 .Alias "age_x2")
    let withColumn (expr: Expr) (lf: LazyFrame) : LazyFrame =
        let handles = [| expr.Handle |]
        let h = PolarsWrapper.LazyWithColumns(lf.Handle, handles)
        new LazyFrame(h)

    // [新增] WithColumns (添加多列)
    let withColumns (exprs: Expr list) (lf: LazyFrame) : LazyFrame =
        let handles = exprs |> List.map (fun e -> e.Handle) |> List.toArray
        let h = PolarsWrapper.LazyWithColumns(lf.Handle, handles)
        new LazyFrame(h)

    // 触发执行
    let collect (lf: LazyFrame) : DataFrame = lf.Collect()

// ==========================================
// 3. Main
// ==========================================
module App =
    [<EntryPoint>]
    let main argv =
        printfn "F# Safety Check Starting..."

        try
            // 故意读取一个不存在的文件 (或者你可以造一个格式错误的 CSV)
            // 注意：我们在 C# Wrapper 层有一层 File.Exists 检查，
            // 为了测试 Rust 的报错，我们可以造一个列名错误的查询。
            
            let rootDir = AppDomain.CurrentDomain.BaseDirectory
            let dateCsvPath = Path.GetFullPath(Path.Combine(rootDir, "../../../dates.csv"))
            // 确保文件存在
            if not (File.Exists(dateCsvPath)) then File.WriteAllText(dateCsvPath, "a,b\n1,2")

            printfn "--- Test 1: Panic Safety (Wrong Column) ---"
            let df = Polars.readCsv dateCsvPath None
            
            printfn "Attempting to filter by non-existent column 'Z'..."
            // 这在以前会导致 panic/abort，现在应该抛出 F# 异常
            df 
            |> Polars.filter (Polars.col "Z" .== Polars.lit 1)
            |> Polars.show 
            |> ignore

        with ex -> 
            // 我们期望捕获到异常，而不是程序直接消失
            printfn "\n✅ CAUGHT EXCEPTION SUCCESSFULLY!"
            printfn "Error Message: %s" ex.Message

        0