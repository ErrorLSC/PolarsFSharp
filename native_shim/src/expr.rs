use polars::prelude::*;
use std::os::raw::c_char;
use std::ops::Mul;
use crate::types::{ExprContext, ptr_to_str};

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_col(name_ptr: *const c_char) -> *mut ExprContext {
    let name = ptr_to_str(name_ptr).unwrap();
    // Polars API: col("name")
    let expr = col(name); 
    Box::into_raw(Box::new(ExprContext { inner: expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_lit_i32(val: i32) -> *mut ExprContext {
    // Polars API: lit(10)
    let expr = lit(val);
    Box::into_raw(Box::new(ExprContext { inner: expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_lit_str(val_ptr: *const c_char) -> *mut ExprContext {
    let val = ptr_to_str(val_ptr).unwrap();
    // Polars API: lit("string")
    let expr = lit(val);
    Box::into_raw(Box::new(ExprContext { inner: expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_lit_f64(val: f64) -> *mut ExprContext {
    let expr = lit(val);
    Box::into_raw(Box::new(ExprContext { inner: expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_eq(left_ptr: *mut ExprContext, right_ptr: *mut ExprContext) -> *mut ExprContext {
    let left = unsafe { Box::from_raw(left_ptr) };
    let right = unsafe { Box::from_raw(right_ptr) };
    
    // Polars API: left.eq(right)
    let new_expr = left.inner.eq(right.inner);
    
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_gt(left_ptr: *mut ExprContext, right_ptr: *mut ExprContext) -> *mut ExprContext {
    // 拿到两个积木
    let left = unsafe { Box::from_raw(left_ptr) };
    let right = unsafe { Box::from_raw(right_ptr) };
    
    // 组合它们: left.gt(right)
    // 注意：Polars 的运算符重载会消耗所有权，所以我们正好用了 Box::from_raw
    let new_expr = left.inner.gt(right.inner);
    
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_mul(left_ptr: *mut ExprContext, right_ptr: *mut ExprContext) -> *mut ExprContext {
    let left = unsafe { Box::from_raw(left_ptr) };
    let right = unsafe { Box::from_raw(right_ptr) };
    // left * right
    let new_expr = left.inner.mul(right.inner); 
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_alias(expr_ptr: *mut ExprContext, name_ptr: *const c_char) -> *mut ExprContext {
    let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
    let name = ptr_to_str(name_ptr).unwrap();
    // expr.alias("new_name")
    let new_expr = expr_ctx.inner.alias(name);
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_sum(ptr: *mut ExprContext) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(ptr) };
    let new_expr = ctx.inner.sum();
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_mean(ptr: *mut ExprContext) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(ptr) };
    let new_expr = ctx.inner.mean();
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_max(ptr: *mut ExprContext) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(ptr) };
    let new_expr = ctx.inner.max();
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

// ==========================================
// String Operations (例如 Contains)
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_contains(
    expr_ptr: *mut ExprContext, 
    pat_ptr: *const c_char
) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(expr_ptr) };
    let pat = ptr_to_str(pat_ptr).unwrap();
    
    // Polars API: col("a").str().contains(lit("pattern"), strict=false)
    // 注意：0.50 API 可能会变，通常在 str() 命名空间下
    // strict=true 意味着如果不是字符串类型会报错
    let new_expr = ctx.inner.str().contains(lit(pat), false);
    
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

// ==========================================
// 时间序列 (Temporal Ops)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dt_year(ptr: *mut ExprContext) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(ptr) };
    
    // Polars API: col("date").dt().year()
    // 注意：.dt() 进入日期命名空间
    let new_expr = ctx.inner.dt().year();
    
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}