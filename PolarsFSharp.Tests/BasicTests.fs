namespace PolarsFSharp.Tests

open Xunit
open PolarsFSharp

type ``Basic Functionality Tests`` () =

    [<Fact>]
    member _.``Can read CSV and count rows/cols`` () =
        use csv = new TempCsv("name,age,birthday\nAlice,30,2022-11-01\nBob,25,2025-12-03")
        
        let df = Polars.readCsv csv.Path None
        
        Assert.Equal(2L, df.Rows)    // 注意：现在 Rows 返回的是 long (int64)
        Assert.Equal(3L, df.Columns) // 注意：现在 Columns 返回的是 long

    [<Fact>]
    member _.``Can read Parquet`` () =
        // 这一步需要你有一个真实的 parquet 文件，或者先用 writeParquet 生成一个
        use csv = new TempCsv("a,b\n1,2")
        let df = Polars.readCsv csv.Path None
        
        let tmpParquet = System.IO.Path.GetTempFileName()
        try
            // 测试 Write -> Read 闭环
            df |> Polars.writeParquet tmpParquet |> ignore
            let df2 = Polars.readParquet tmpParquet
            Assert.Equal(df.Rows, df2.Rows)
        finally
            System.IO.File.Delete(tmpParquet)