using System;
using Polars.Native;

namespace Polars.CSharp;

/// <summary>
/// A Polars Expr
/// </summary>
public class Expr : IDisposable
{
    internal ExprHandle Handle { get; }

    internal Expr(ExprHandle handle)
    {
        Handle = handle;
    }

    /// <summary>
    /// Creates an expression evaluating if the left operand is greater than the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    public static Expr operator >(Expr left, Expr right)
    {
        var l = PolarsWrapper.CloneExpr(left.Handle);
        var r = PolarsWrapper.CloneExpr(right.Handle);
        return new Expr(PolarsWrapper.Gt(l, r));
    }

    /// <summary>
    /// Creates an expression evaluating if the left operand is less than the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    public static Expr operator <(Expr left, Expr right)
    {
        var l = PolarsWrapper.CloneExpr(left.Handle);
        var r = PolarsWrapper.CloneExpr(right.Handle);
        return new Expr(PolarsWrapper.Lt(l, r));
    }

    /// <summary>
    /// Creates an expression evaluating if the left operand is greater than or equal to the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    public static Expr operator >=(Expr left, Expr right)
    {
        var l = PolarsWrapper.CloneExpr(left.Handle);
        var r = PolarsWrapper.CloneExpr(right.Handle);
        return new Expr(PolarsWrapper.GtEq(l, r)); 
    }

    /// <summary>
    /// Creates an expression evaluating if the left operand is less than or equal to the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    public static Expr operator <=(Expr left, Expr right)
    {
        var l = PolarsWrapper.CloneExpr(left.Handle);
        var r = PolarsWrapper.CloneExpr(right.Handle);
        return new Expr(PolarsWrapper.LtEq(l, r)); 
    }

    /// <summary>
    /// Creates an expression evaluating if the left operand is equal to the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    public static Expr operator ==(Expr left, Expr right)
    {
        var l = PolarsWrapper.CloneExpr(left.Handle);
        var r = PolarsWrapper.CloneExpr(right.Handle);
        return new Expr(PolarsWrapper.Eq(l, r));   
    }

    /// <summary>
    /// Creates an expression evaluating if the left operand is not equal to the right operand.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A boolean expression representing the comparison.</returns>
    public static Expr operator !=(Expr left, Expr right)
    {
        var l = PolarsWrapper.CloneExpr(left.Handle);
        var r = PolarsWrapper.CloneExpr(right.Handle);
        return new Expr(PolarsWrapper.Neq(l, r));  
    }

    // ==========================================
    // Arithmetic Operators
    // ==========================================

    /// <summary>
    /// Creates an expression representing the addition of two expressions.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the sum.</returns>
    public static Expr operator +(Expr left, Expr right) => 
        new(PolarsWrapper.Add(left.Handle, right.Handle));

    /// <summary>
    /// Creates an expression representing the subtraction of the right expression from the left expression.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the difference.</returns>
    public static Expr operator -(Expr left, Expr right) => 
        new(PolarsWrapper.Sub(left.Handle, right.Handle));

    /// <summary>
    /// Creates an expression representing the multiplication of two expressions.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the product.</returns>
    public static Expr operator *(Expr left, Expr right) => 
        new(PolarsWrapper.Mul(left.Handle, right.Handle));

    /// <summary>
    /// Creates an expression representing the division of the left expression by the right expression.
    /// </summary>
    /// <param name="left">The left expression.</param>
    /// <param name="right">The right expression.</param>
    /// <returns>A numeric expression representing the quotient.</returns>
    public static Expr operator /(Expr left, Expr right) => 
        new(PolarsWrapper.Div(left.Handle, right.Handle));
    
    // ==========================================
    // Logical Operators
    // ==========================================

    /// <summary>
    /// Creates an expression representing the logical AND operation.
    /// </summary>
    /// <param name="left">The left boolean expression.</param>
    /// <param name="right">The right boolean expression.</param>
    /// <returns>A boolean expression that evaluates to true if both operands are true.</returns>
    public static Expr operator &(Expr left, Expr right) => 
        new(PolarsWrapper.And(left.Handle, right.Handle));

    /// <summary>
    /// Creates an expression representing the logical OR operation.
    /// </summary>
    /// <param name="left">The left boolean expression.</param>
    /// <param name="right">The right boolean expression.</param>
    /// <returns>A boolean expression that evaluates to true if at least one operand is true.</returns>
    public static Expr operator |(Expr left, Expr right) => 
        new(PolarsWrapper.Or(left.Handle, right.Handle));

    /// <summary>
    /// Creates an expression representing the logical NOT operation.
    /// </summary>
    /// <param name="expr">The boolean expression to negate.</param>
    /// <returns>A boolean expression that evaluates to the opposite truth value.</returns>
    public static Expr operator !(Expr expr) => 
        new(PolarsWrapper.Not(expr.Handle));

    // ---------------------------------------------------
    // 基础方法
    // ---------------------------------------------------

    /// <summary>
    /// Set a new name for a column
    /// </summary>
    /// <param name="name"></param>
    /// <returns></returns>
    public Expr Alias(string name) => 
        new(PolarsWrapper.Alias(Handle, name));

    // ==========================================
    // Aggregation (聚合函数)
    // ==========================================

    /// <summary>
    /// Sum
    /// </summary>
    public Expr Sum()
    {
        // 1. 克隆 Handle (防止当前对象被底层消耗)
        var cloned = PolarsWrapper.CloneExpr(Handle);
        // 2. 调用底层 Wrapper
        return new Expr(PolarsWrapper.Sum(cloned));
    }

    /// <summary>
    /// Mean
    /// </summary>
    public Expr Mean()
    {
        var cloned = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.Mean(cloned));
    }

    /// <summary>
    /// Max
    /// </summary>
    public Expr Max()
    {
        var cloned = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.Max(cloned));
    }

    /// <summary>
    /// Min
    /// </summary>
    public Expr Min()
    {
        var cloned = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.Min(cloned));
    }

    // ==========================================
    // Math
    // ==========================================

    /// <summary>
    /// Abs
    /// </summary>
    public Expr Abs()
    {
        var cloned = PolarsWrapper.CloneExpr(Handle);
        return new Expr(PolarsWrapper.Abs(cloned));
    }
    
    // ---------------------------------------------------
    // Clean Up
    // ---------------------------------------------------
    /// <summary>
    /// Dispose a handle.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
    }

    /// <summary>
    /// 判断当前 Expr 对象是否与另一个对象是同一个实例（基于底层指针地址）。
    /// <para>注意：Polars 的 Expr 是不透明指针。即使逻辑相同的两个表达式（如 col("a") 和 col("a")），
    /// 它们在底层也是不同的内存对象，因此 Equals 会返回 false。</para>
    /// </summary>
    public override bool Equals(object? obj)
    {
        // 1. 快速检查引用是否指向同一托管对象
        if (ReferenceEquals(this, obj)) return true;

        // 2. 类型检查 (Pattern Matching)
        if (obj is not Expr other) return false;

        // 3. 句柄有效性检查 (防止操作已释放的句柄)
        if (this.Handle.IsInvalid || other.Handle.IsInvalid) return false;

        // 4. 严谨的底层指针比较
        // 两个 Expr 即使逻辑相同，如果它们对应 Rust 侧不同的内存地址，也被视为不等。
        // 这对于防止 Dictionary<Expr, T> 产生意外行为至关重要。
        return this.Handle.DangerousGetHandle() == other.Handle.DangerousGetHandle();
    }

    /// <summary>
    /// 获取基于底层指针地址的哈希码。
    /// </summary>
    public override int GetHashCode()
    {
        // 如果句柄无效，返回 0 或抛出异常（这里选择返回 0 以保证稳定性）
        if (Handle.IsInvalid) return 0;
        
        // 使用 IntPtr 自身的 GetHashCode，它是基于内存地址的
        return Handle.DangerousGetHandle().GetHashCode();
    }
}