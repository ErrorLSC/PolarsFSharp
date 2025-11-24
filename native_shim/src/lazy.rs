use polars::prelude::*;
use crate::types::*;

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_filter(
    lf_ptr: *mut LazyFrameContext, 
    expr_ptr: *mut ExprContext
) -> *mut LazyFrameContext {
    // 拿回所有权 (Consume)
    let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
    let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
    
    // 执行 lazy filter (这一步很快，只是修改逻辑计划)
    let new_lf = lf_ctx.inner.filter(expr_ctx.inner);
    
    Box::into_raw(Box::new(LazyFrameContext { inner: new_lf }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_select(
    lf_ptr: *mut LazyFrameContext,
    exprs_ptr: *const *mut ExprContext,
    len: usize
) -> *mut LazyFrameContext {
    let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
    
    let mut exprs = Vec::with_capacity(len);
    unsafe {
        for &p in std::slice::from_raw_parts(exprs_ptr, len) {
            exprs.push(Box::from_raw(p).inner);
        }
    }

    let new_lf = lf_ctx.inner.select(exprs);
    Box::into_raw(Box::new(LazyFrameContext { inner: new_lf }))
}

// 补充一个 sort (order_by)
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sort(
    lf_ptr: *mut LazyFrameContext,
    expr_ptr: *mut ExprContext,
    descending: bool
) -> *mut LazyFrameContext {
    let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
    let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
    
    // sort_by_exprs
    let new_lf = lf_ctx.inner.sort_by_exprs(
        vec![expr_ctx.inner], 
        SortMultipleOptions::default().with_order_descending(descending)
    );
    Box::into_raw(Box::new(LazyFrameContext { inner: new_lf }))
}

// ==========================================
// Collect (出口：LazyFrame -> DataFrame)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_collect(lf_ptr: *mut LazyFrameContext) -> *mut DataFrameContext {
    let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
    println!("(Rust) Collecting LazyFrame... (Optimizer engaging)");

    match lf_ctx.inner.collect() {
        Ok(df) => {
            println!("(Rust) Collect done. Shape: {:?}", df.shape());
            Box::into_raw(Box::new(DataFrameContext { df }))
        },
        Err(e) => {
            println!("(Rust) Collect failed: {}", e);
            std::ptr::null_mut()
        }
    }
}






// ==========================================
// Limit / Head (截取前 N 行)
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_limit(lf_ptr: *mut LazyFrameContext, n: u32) -> *mut LazyFrameContext {
    let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
    // Polars API: limit(n)
    let new_lf = lf_ctx.inner.limit(n); 
    Box::into_raw(Box::new(LazyFrameContext { inner: new_lf }))
}

// ==========================================
// WithColumns (添加列)
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_with_columns(
    lf_ptr: *mut LazyFrameContext,
    exprs_ptr: *const *mut ExprContext,
    len: usize
) -> *mut LazyFrameContext {
    let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
    
    let mut exprs = Vec::with_capacity(len);
    unsafe {
        for &p in std::slice::from_raw_parts(exprs_ptr, len) {
            exprs.push(Box::from_raw(p).inner);
        }
    }

    let new_lf = lf_ctx.inner.with_columns(exprs);
    Box::into_raw(Box::new(LazyFrameContext { inner: new_lf }))
}