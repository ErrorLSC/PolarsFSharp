namespace PolarsFSharp

open System
open Polars.Native

// ==========================================
// Expr 类型封装
// ==========================================
type Expr(handle: ExprHandle) =
    member _.Handle = handle
    member internal this.CloneHandle() = PolarsWrapper.CloneExpr(handle)
    // 运算符重载
    static member (.>) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Gt(lhs.Handle, rhs.Handle))
    static member (.==) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Eq(lhs.Handle, rhs.Handle))
    static member (.*) (lhs: Expr, rhs: Expr) = new Expr(PolarsWrapper.Mul(lhs.Handle, rhs.Handle))
    
    // 方法
    member this.Alias(name: string) = new Expr(PolarsWrapper.Alias(handle, name))
    member this.Sum() = new Expr(PolarsWrapper.Sum(handle))
    member this.Mean() = new Expr(PolarsWrapper.Mean(handle))
    member this.Max() = new Expr(PolarsWrapper.Max(handle))
    member this.Min() = new Expr(PolarsWrapper.Min(handle))
    member this.StrContains(pattern: string) = new Expr(PolarsWrapper.StrContains(handle, pattern))

    member this.Dt = new DtOps(handle)

and DtOps(handle: ExprHandle) =
    member _.Year() = new Expr(PolarsWrapper.DtYear(handle))

// DataFrame 封装
type DataFrame(handle: DataFrameHandle) =
    interface IDisposable with
        member _.Dispose() = handle.Dispose()
    
    member _.Handle = handle
    
    // 依然保留，用于 Show 或者用户真的需要 Arrow 数据时
    member this.ToArrow() = PolarsWrapper.Collect(handle)

    // 零拷贝获取行数
    member _.Rows = PolarsWrapper.DataFrameHeight(handle)

    // 零拷贝获取列数
    member _.Columns = PolarsWrapper.DataFrameWidth(handle)

// LazyFrame 封装
// 它依赖 DataFrame (Collect 返回 DataFrame)，所以必须定义在 DataFrame 后面
type LazyFrame(handle: LazyFrameHandle) =
    member _.Handle = handle
    member internal this.CloneHandle() = PolarsWrapper.CloneLazy(handle)
    member this.Collect() = 
        let dfHandle = PolarsWrapper.LazyCollect(handle)
        new DataFrame(dfHandle)