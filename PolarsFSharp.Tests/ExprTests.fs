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
