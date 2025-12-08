namespace Polars.FSharp.Tests

module UdfLogic =
    open Apache.Arrow
    open Apache.Arrow.Types

    // 场景 A: 类型转换 (Int32 -> String)
    let intToString (arr: IArrowArray) : IArrowArray =
        match arr with
        | :? Int64Array as i64Arr ->
            let builder = new StringViewArray.Builder()
            for i in 0 .. i64Arr.Length - 1 do
                if i64Arr.IsNull(i) then builder.AppendNull() |> ignore
                else 
                    let v = i64Arr.GetValue(i).Value
                    builder.Append $"Value: {v}" |> ignore
            builder.Build() :> IArrowArray

    // 为了兼容性保留 Int32
        | :? Int32Array as i32Arr ->
            let builder = new StringViewArray.Builder()
            for i in 0 .. i32Arr.Length - 1 do
                if i32Arr.IsNull(i) then 
                    builder.AppendNull() |> ignore
                else 
                    let v = i32Arr.GetValue(i).Value
                    builder.Append $"Value: {v}" |> ignore
            builder.Build() :> IArrowArray
        |_ -> failwith $"Expected Int32Array or Int64Array, but got: {arr.GetType().Name}"

    // 场景 B: 必定报错
    let alwaysFail (arr: IArrowArray) : IArrowArray =
        failwith "Boom! C# UDF Exploded!"

open Xunit
open Polars.FSharp
open Apache.Arrow
open System
open Polars.Native

type ``UDF Tests`` () =

    [<Fact>]
    member _.``Map UDF can change data type (Int -> String)`` () =
        // 1. 准备数据
        use csv = new TempCsv "num\n100\n200"
        let lf = Polars.scanCsv csv.Path None
        
        // 2. 构造 C# 委托
        let udf = Func<IArrowArray, IArrowArray> UdfLogic.intToString

        // 3. 执行 Polars 查询
        // 关键点：必须传入 PlDataType.String，否则 Polars 可能会把结果当成 Int 处理导致乱码
        let df = 
            lf 
            |> Polars.withColumnLazy (
                Polars.col "num"
                |> fun e -> e.Map(udf, PlDataType.String)
                |> Polars.alias "desc"
            )
            |> Polars.selectLazy [ Polars.col "desc" ]
            |> Polars.collect

        // 4. 验证结果
        let arrowBatch = df.ToArrow()
        let strCol = arrowBatch.Column "desc" :?> StringViewArray
        
        Assert.Equal("Value: 100", strCol.GetString 0)
        Assert.Equal("Value: 200", strCol.GetString 1)

    [<Fact>]
    member _.``Map UDF error is propagated to F#`` () =
        // 1. 准备数据
        use csv = new TempCsv("num\n1")
        let lf = Polars.scanCsv csv.Path None
        
        let udf = System.Func<IArrowArray, IArrowArray> UdfLogic.alwaysFail

        // 2. 断言会抛出异常
        let ex = Assert.Throws<Exception>(fun () -> 
            lf 
            |> Polars.withColumnLazy (
                Polars.col "num" 
                |> fun e -> e.Map(udf, PlDataType.SameAsInput)
            )
            // UDF 是 Lazy 执行的，只有 Collect/ToArrow 时才会触发
            |> Polars.collect 
            |> ignore
        )

        // 3. 验证异常信息
        // 我们期望看到 C# 的异常信息包含在 PolarsError 里
        Assert.Contains("Boom! C# UDF Exploded!", ex.Message)
        Assert.Contains("C# UDF Failed", ex.Message) // 这是 Rust 代码里加的前缀

    [<Fact>]
    member _.``Generic Map UDF with Lambda (Int -> String)`` () =
        use csv = new TempCsv "num\n100\n"
        let lf = Polars.scanCsv csv.Path None
        
        // --- 用户的代码极度简化 ---
        // 1. 定义一个简单的匿名函数 (int -> string)
        let myLogic = fun (x: int) -> sprintf "Num: %d" (x + 1)

        let df = 
            lf 
            |> Polars.withColumnLazy (
                Polars.col "num"
                // 2. 直接调用 Udf.map
                // 泛型 'T 和 'U 会自动推断为 int 和 string
                |> fun e -> e.Map(Udf.map myLogic DataType.String, PlDataType.String) 
                |> Polars.alias "res"
            )
            |> Polars.selectLazy [ Polars.col "res" ]
            |> Polars.collect

        // 验证
        let arrow = df.ToArrow()
        let col = arrow.Column "res" :?> StringViewArray // 自动用了 StringView
        
        Assert.Equal("Num: 101", col.GetString 0)
        Assert.Equal(1, col.Length)
    [<Fact>]
    member _.``UDF: Map with Option (Null Handling)`` () =
        // 数据: [10, 20, null]
        // 注意: CSV 最后一行写 null 还是空行取决于解析器，这里用空行配合 Polars 默认行为
        use csv = new TempCsv "val\n10\n20\n" 
        let lf = Polars.scanCsv csv.Path None

        // 逻辑: 
        // 输入是 int option
        // 如果有值且 > 15 -> 返回 Some (x * 2)
        // 否则 (<= 15 或原本就是 null) -> 返回 None (即 null)
        let logic (opt: int option) =
            match opt with
            | Some x when x > 15 -> Some (x * 2)
            | _ -> None

        let df = 
            lf 
            |> Polars.withColumnLazy (
                Polars.col "val"
                // 使用 mapOption，显式处理 Option 类型
                |> fun e -> e.Map(Udf.mapOption logic DataType.Int32, PlDataType.Int32)
                |> Polars.alias "res"
            )
            |> Polars.collect

        // 验证
        let arrow = df.ToArrow()
        let col = arrow.Column "res" :?> Int32Array // 注意根据 DataType.Int32 生成的是 Int32Array

        // Row 0: 10 -> (<=15) -> None
        Assert.True(col.IsNull 0)

        // Row 1: 20 -> (>15) -> 40
        Assert.Equal(40, col.GetValue(1).Value)

        // Row 2: null -> (None) -> None
        Assert.True(col.IsNull 2)