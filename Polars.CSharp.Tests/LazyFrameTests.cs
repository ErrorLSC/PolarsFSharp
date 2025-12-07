using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class LazyFrameTests
{
    [Fact]
    public void Test_Lazy_Concat_Horizontal_And_Safety()
    {
        // 准备两个 LazyFrame
        using var csv1 = new DisposableCsv("id\n1\n2");
        using var lf1 = LazyFrame.ScanCsv(csv1.Path);

        using var csv2 = new DisposableCsv("name\nAlice\nBob");
        using var lf2 = LazyFrame.ScanCsv(csv2.Path);

        // 1. 执行 Horizontal Concat
        var concatLf = LazyFrame.Concat([lf1, lf2], ConcatType.Horizontal);
        
        // 收集结果
        using var df = concatLf.Collect();
        
        Assert.Equal(2, df.Height);
        Assert.Equal(2, df.Width); // id, name

        // 验证数据
        using var batch = df.ToArrow();
        Assert.Equal(1, batch.Column("id").GetInt64Value(0));
        Assert.Equal("Alice", batch.Column("name").GetStringValue(0));

        // 2. [关键] 验证 C# 对象安全性 (F# vs C# 习惯测试)
        
        // 我们复用 lf1 做另一个查询
        using var df1_again = lf1.Select(Col("id") * Lit(10)).Collect();
        Assert.Equal(2, df1_again.Height);
        
        using var batch1 = df1_again.ToArrow();
        Assert.Equal(10, batch1.Column("id").GetInt64Value(0));
    }
    
    [Fact]
    public void Test_Lazy_Concat_Diagonal()
    {
        // LF1: [A, B]
        using var csv1 = new DisposableCsv("A,B\n1,10");
        using var lf1 = LazyFrame.ScanCsv(csv1.Path);

        // LF2: [B, C]
        using var csv2 = new DisposableCsv("B,C\n20,300");
        using var lf2 = LazyFrame.ScanCsv(csv2.Path);

        // Diagonal Concat (Lazy)
        var concatLf = LazyFrame.Concat(new[] { lf1, lf2 }, ConcatType.Diagonal);
        
        using var df = concatLf.Collect();
        
        Assert.Equal(2, df.Height);
        Assert.Equal(3, df.Width); // A, B, C

        using var batch = df.ToArrow();
        // 验证第一行 (来自 LF1) -> C 应该是 null
        Assert.True(batch.Column("C").IsNull(0));
        
        // 验证第二行 (来自 LF2) -> A 应该是 null
        Assert.True(batch.Column("A").IsNull(1));
    }
}