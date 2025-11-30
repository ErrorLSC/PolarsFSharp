namespace PolarsFSharp.Tests

open Xunit
open PolarsFSharp

type ``Complex Query Tests`` () =
    
    [<Fact>]
    member _.``Join execution (Eager)`` () =
        use users = new TempCsv("id,name\n1,A\n2,B")
        use sales = new TempCsv("uid,amt\n1,100\n1,200\n3,50")

        let uDf = Polars.readCsv users.Path None
        let sDf = Polars.readCsv sales.Path None

        let res = 
            uDf 
            |> Polars.join sDf [Polars.col "id"] [Polars.col "uid"] "left"
        
        // Left join: id 1 (2 rows), id 2 (1 row null match) -> Total 3
        Assert.Equal(3L, res.Rows)

    [<Fact>]
    member _.``Lazy API Chain (Filter -> Collect)`` () =
        use csv = new TempCsv("a,b\n1,10\n2,20\n3,30")
        let lf = Polars.scanCsv csv.Path None
        
        let df = 
            lf
            |> Polars.filterLazy (Polars.col "a" .> Polars.lit 1)
            |> Polars.limit 1u
            |> Polars.collect

        Assert.Equal(1L, df.Rows)

    [<Fact>]
    member _.``GroupBy Queries`` () =
        use csv = new TempCsv("name,birthdate,weight,height\nBen Brown,1985-02-15,72.5,1.77\nQinglei,2025-11-25,70.0,1.80\nZhang,2025-10-31,55,1.75")
        let lf = Polars.scanCsv csv.Path (Some true)

        let res = 
            lf 
            |> Polars.groupByLazy
                [(Polars.col "birthdate").Dt.Year() / Polars.lit 10 * Polars.lit 10 |> Polars.alias "decade" ]
                [ Polars.count().Alias("cnt")] 
            |> Polars.sortLazy (Polars.col "decade") false
            |> Polars.collect

        // 验证
        // Row 0: 1980 -> 2
        Assert.Equal(1980L, res.Int("decade", 0).Value)
        // 注意：count() 返回通常是 UInt32 或 UInt64，我们用 Int64 读取是安全的
        Assert.Equal(1L, int64 (res.Int("cnt", 0).Value)) 

        // Row 1: 1990 -> 1
        Assert.Equal(2020L, res.Int("decade", 1).Value)
        Assert.Equal(2L, int64 (res.Int("cnt", 1).Value))

    [<Fact>]
    member _.``Complex Transformation (Selector Exclude)`` () =
        use csv = new TempCsv("name,weight,height,ignore_me\nZhang,70,1.75,999")
        let lf = Polars.scanCsv csv.Path None

        let res = 
            lf
            |> Polars.withColumn (
                // 1. String Split -> List -> First
                // "Zhang San" -> ["Zhang", "San"] -> "Zhang"
                (Polars.col "name").Str.Split(" ").List.First().Alias("first_name")
            )
            |> Polars.selectLazy [
                // 2. Exclude (all except "ignore_me")
                Polars.all() |> Polars.exclude ["ignore_me"] |> Polars.asExpr
            ]
            |> Polars.withColumns [
                // 3. Round & Prefix
                // 对 weight 四舍五入并加前缀
                (Polars.col "weight").Round(2).Name.Prefix("avg_")
            ]
            |> Polars.collect

        Assert.DoesNotContain("ignore_me", res.ColumnNames)
