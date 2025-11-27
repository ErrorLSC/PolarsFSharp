namespace PolarsFSharp.Tests

open Xunit
open PolarsFSharp

type ``Expression Logic Tests`` () =

    [<Fact>]
    member _.``Filter by numeric value (> operator)`` () =
        use csv = new TempCsv("val\n10\n20\n30")
        let df = Polars.readCsv csv.Path None
        
        let res = df |> Polars.filter (Polars.col "val" .> Polars.lit 15)
        
        Assert.Equal(2L, res.Rows)

    [<Fact>]
    member _.``Filter by string value (== operator)`` () =
        use csv = new TempCsv("name\nAlice\nBob\nAlice")
        let df = Polars.readCsv csv.Path None
        
        // SRTP 魔法测试
        let res = df |> Polars.filter (Polars.col "name" .== Polars.lit "Alice")
        
        Assert.Equal(2L, res.Rows)

    [<Fact>]
    member _.``Filter by double value (== operator)`` () =
        use csv = new TempCsv("value\n3.36\n4.2\n5\n3.36")
        let df = Polars.readCsv csv.Path None
        
        // SRTP 魔法测试
        let res = df |> Polars.filter (Polars.col "value" .== Polars.lit 3.36)
        
        Assert.Equal(2L, res.Rows)

    [<Fact>]
    member _.``Null handling works`` () =
        // 造一个带 null 的 CSV
        // age: 10, null, 30
        use csv = new TempCsv("age\n10\n\n30") 
        let lf = Polars.scanCsv csv.Path None

        // 测试 1: fill_null
        // 把 null 填成 0，然后筛选 age > 0
        // 结果应该是 3 行 (10, 0, 30)
        let res = 
            lf 
            |> Polars.withColumn (
                Polars.col "age" 
                |> Polars.fillNull (Polars.lit 0) 
                |> Polars.alias "age_filled"
            )
            |> Polars.filterLazy (Polars.col "age_filled" .>= Polars.lit 0)
            |> Polars.collect
        Assert.Equal(3L, res.Rows)
        
        // 测试 2: is_null
        // 筛选出 null 的行
        let df= Polars.readCsv csv.Path None 
        let nulls = df |> Polars.filter (Polars.col "age" |> Polars.isNull)
        Assert.Equal(1L, nulls.Rows)
