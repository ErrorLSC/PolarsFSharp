#nullable enable
using Xunit;
using Polars.CSharp;
using Apache.Arrow;

namespace Polars.CSharp.Tests;

public class SeriesTests
{
    [Fact]
    public void Test_Series_Creation_And_Arrow()
    {
        // 1. 创建 Series (Int32)
        using var s = new Series("my_series", [1, 2, 3]);
        
        Assert.Equal(3, s.Length);
        Assert.Equal("my_series", s.Name);

        // 2. 转 Arrow
        var arrowArray = s.ToArrow();
        Assert.IsType<Int32Array>(arrowArray);
        Assert.Equal(2, ((Int32Array)arrowArray).GetValue(1));

        // 3. Rename
        s.Name = "renamed";
        Assert.Equal("renamed", s.Name);
    }

    [Fact]
    public void Test_Series_String_And_Nulls()
    {
        // 1. 创建 String Series (带 Null)
        using var s = new Series("strings", ["a", null, "c"]);
        
        Assert.Equal(3, s.Length);
        
        // Polars 0.50 默认可能是 StringViewArray 或 LargeStringArray
        // 我们用之前的扩展方法来验证
        Assert.Equal("a", s.GetValue<string>(0));
        Assert.Null(s.GetValue<string>(1));
        Assert.Equal("c", s.GetValue<string>(2));
    }

    [Fact]
    public void Test_Series_Cast_Decimal()
    {
        // 1. 创建 Double Series
        using var s = new Series("prices", [10.5, 20.0]);

        // 2. Cast 到 Decimal(10, 2)
        // 这需要 DataType 类发挥作用
        using var sDecimal = s.Cast(DataType.Decimal(10, 2));
        
        // 验证 Cast 后的 Arrow 类型
        var arrowArray = sDecimal.ToArrow();
        // Apache Arrow C# 会把 Decimal128 映射为 Decimal128Array
        Assert.IsType<Decimal128Array>(arrowArray);
    }
    [Fact]
    public void Test_NullCount()
    {
        // Case 1: 整数 Series (含 Null)
        using var sInt = new Series("nums", [1, null, 3, null, 5]);
        
        // 验证: 应该有 2 个 null
        Assert.Equal(2, sInt.NullCount);
        Assert.Equal(5, sInt.Length);

        // Case 2: 字符串 Series (含 Null)
        using var sStr = new Series("str", ["a", null, "b"]);
        
        // 验证: 应该有 1 个 null
        Assert.Equal(1, sStr.NullCount);
        
        // Case 3: 全是 Null
        using var sAllNull = new Series("nulls", new string?[] { null, null });
        Assert.Equal(2, sAllNull.NullCount);
        
        // Case 4: 没有 Null
        using var sClean = new Series("clean", [1, 2, 3]);
        Assert.Equal(0, sClean.NullCount);
    }
    [Fact]
    public void Test_Series_Arithmetic()
    {
        using var s1 = new Series("a", [1, 2, 3]);
        using var s2 = new Series("b", [10, 20, 30]);

        // Test Add (+)
        using var sum = s1 + s2;
        Assert.Equal(11, sum.GetValue<int>(0));
        Assert.Equal(22, sum.GetValue<int>(1));
        Assert.Equal(33, sum.GetValue<int>(2));

        // Test Mul (*)
        using var prod = s1 * s2;
        Assert.Equal(10, prod.GetValue<int>(0));
        Assert.Equal(90, prod.GetValue<int>(2)); // 3 * 30
    }

    [Fact]
    public void Test_Series_Comparison()
    {
        using var s1 = new Series("a", [1, 5, 10]);
        using var s2 = new Series("b", [1, 4, 20]);

        // Test Eq (1==1, 5!=4, 10!=20) -> [true, false, false]
        using var eq = s1.Eq(s2);
        Assert.True(eq.GetValue<bool>(0));
        Assert.False(eq.GetValue<bool>(1));

        // Test Gt (>) (1>1 false, 5>4 true, 10>20 false)
        using var gt = s1 > s2;
        Assert.False(gt.GetValue<bool>(0));
        Assert.True(gt.GetValue<bool>(1));
        Assert.False(gt.GetValue<bool>(2));
    }

    [Fact]
    public void Test_Series_Aggregations()
    {
        using var s = new Series("nums", [1, 2, 3, 4, 5]);

        // Sum: 15
        using var sumSeries = s.Sum();
        Assert.Equal(1, sumSeries.Length); // 聚合后长度为 1
        Assert.Equal(15, sumSeries.GetValue<int>(0));
        
        // 验证泛型快捷方法
        Assert.Equal(15, s.Sum<int>());

        // Mean: 3.0
        // 注意：Mean 可能会返回 double，具体取决于 Polars 内部实现，通常是 float64
        Assert.Equal(3.0, s.Mean<double>());
        
        // Min/Max
        Assert.Equal(1, s.Min<int>());
        Assert.Equal(5, s.Max<int>());
    }

    [Fact]
    public void Test_Series_FloatChecks()
    {
        // 构造包含 NaN 和 Inf 的 Series
        // C# double.NaN 对应 Polars Float64 NaN
        using var s = new Series("f", [1.0, double.NaN, double.PositiveInfinity]);

        // IsNan -> [false, true, false]
        using var isNan = s.IsNan();
        Assert.False(isNan.GetValue<bool>(0));
        Assert.True(isNan.GetValue<bool>(1));
        Assert.False(isNan.GetValue<bool>(2));

        // IsInfinite -> [false, false, true]
        using var isInf = s.IsInfinite();
        Assert.True(isInf.GetValue<bool>(2));
    }
}