namespace Polars.FSharp.Tests

open System
open Xunit
open Polars.FSharp
open Apache.Arrow

type ``Series Tests`` () =

    [<Fact>]
    member _.``Series: Create Int32 with Nulls`` () =
        let data = [Some 1; None; Some 3; Some 42]
        use s = Series.create("nums", data)
        
        Assert.Equal("nums", s.Name)
        Assert.Equal(4L, s.Length)
        
        // 转 Arrow 验证
        let arrow = s.ToArrow() :?> Int32Array
        Assert.Equal(4, arrow.Length)
        Assert.Equal(1, arrow.GetValue(0).Value)
        Assert.True(arrow.IsNull(1)) // Null Check
        Assert.Equal(3, arrow.GetValue(2).Value)

    [<Fact>]
    member _.``Series: Create Strings with Nulls`` () =
        let data = [Some "hello"; None; Some "world"]
        use s = Series.create("strings", data)
        
        let arrow = s.ToArrow() 
        // Polars 0.50+ 默认 StringViewArray，或者 LargeStringArray
        // 这里做个类型匹配
        match arrow with
        | :? StringViewArray as sa ->
            Assert.Equal("hello", sa.GetString(0))
            Assert.True(sa.IsNull(1))
            Assert.Equal("world", sa.GetString(2))
        | :? StringArray as sa -> // Fallback logic
            Assert.Equal("hello", sa.GetString(0))
            Assert.True(sa.IsNull(1))
        | _ -> failwithf "Unexpected arrow type: %s" (arrow.GetType().Name)

    [<Fact>]
    member _.``Series: Rename`` () =
        use s = Series.create("a", [1;2])
        Assert.Equal("a", s.Name)
        
        s.Rename("b") |> ignore
        Assert.Equal("b", s.Name)

    [<Fact>]
    member _.``Series: Float with Nulls`` () =
        let data = [Some 1.5; None; Some 3.14]
        use s = Series.create("floats", data)
        
        let arrow = s.ToArrow() :?> DoubleArray
        Assert.Equal(1.5, arrow.GetValue(0).Value)
        Assert.True(arrow.IsNull(1))
        Assert.Equal(3.14, arrow.GetValue(2).Value)
    [<Fact>]
    member _.``Interop: DataFrame <-> Series`` () =
        // 1. 创建 DataFrame
        use csv = new TempCsv("name,age\nalice,10\nbob,20")
        let df = Polars.readCsv csv.Path None
        
        // 2. 获取 Series (ByName)
        use sName = df.Column("name")
        Assert.Equal("name", sName.Name)
        Assert.Equal(2L, sName.Length)

        // 3. 获取 Series (ByIndex)
        use sAge = df.Column(1)
        Assert.Equal("age", sAge.Name)

        // 4. 索引器语法
        use sAge2 = df.[1]
        Assert.Equal("age", sAge2.Name)

        // 5. Series -> DataFrame
        let dfNew = sAge.ToFrame()
        Assert.Equal(1L, dfNew.Columns)
        Assert.Equal(2L, dfNew.Rows)
        Assert.Equal("age", dfNew.ColumnNames.[0])