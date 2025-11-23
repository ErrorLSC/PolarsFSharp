module Tests

open System
open System.IO
open Xunit
open Polars.Native

//引用我们刚定义的命名空间
open PolarsFSharp 

// 辅助：创建临时 CSV
type TempCsv(content: string) =
    let path = Path.GetTempFileName()
    do File.WriteAllText(path, content)
    member _.Path = path
    interface IDisposable with
        member _.Dispose() = if File.Exists(path) then File.Delete(path)

type ``Basic Functionality Tests`` () =

    [<Fact>]
    member _.``Can read CSV and count rows`` () =
        use csv = new TempCsv("name,age\nAlice,30\nBob,25")
        
        // 参数改为 None (使用默认日期解析)
        let df = Polars.readCsv csv.Path None
        
        Assert.Equal(2, df.Rows)
        Assert.Equal(2, df.Columns)

    [<Fact>]
    member _.``Can filter by numeric value`` () =
        use csv = new TempCsv("val\n10\n20\n30")
        let df = Polars.readCsv csv.Path None
        
        let res = df |> Polars.filter (Polars.col "val" .> Polars.lit 15)
        
        Assert.Equal(2, res.Rows)

    [<Fact>]
    member _.``Can filter by string value`` () =
        use csv = new TempCsv("name\nAlice\nBob\nAlice")
        let df = Polars.readCsv csv.Path None
        
        // SRTP 魔法：lit "Alice"
        let res = df |> Polars.filter (Polars.col "name" .== Polars.lit "Alice")
        
        Assert.Equal(2, res.Rows)

type ``Safety Tests`` () =

    [<Fact>]
    member _.``Throws Exception on invalid column name instead of crashing`` () =
        use csv = new TempCsv("a,b\n1,2")
        let df = Polars.readCsv csv.Path None
        
        let ex = Assert.Throws<Exception>(fun () -> 
            df 
            |> Polars.filter (Polars.col "WrongColumn" .> Polars.lit 1) 
            |> ignore
        )
        Assert.Contains("unable to find column", ex.Message)

type ``Complex Query Tests`` () =
    
    [<Fact>]
    member _.``Lazy Join execution works`` () =
        use users = new TempCsv("id,name\n1,A\n2,B")
        use sales = new TempCsv("uid,amt\n1,100\n1,200\n3,50")

        let uDf = Polars.readCsv users.Path None
        let sDf = Polars.readCsv sales.Path None

        let res = 
            uDf 
            |> Polars.join sDf [Polars.col "id"] [Polars.col "uid"] "left"
        
        Assert.Equal(3, res.Rows)