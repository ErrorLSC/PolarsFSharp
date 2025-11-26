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