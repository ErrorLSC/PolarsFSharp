use polars::prelude::*;
use std::os::raw::c_char;
use crate::types::*;

#[unsafe(no_mangle)]
pub extern "C" fn pl_select(
    df_ptr: *mut DataFrameContext, 
    exprs_ptr: *const *mut ExprContext, // 这是一个指向指针数组的指针 (ExprHandle[])
    len: usize                          // 数组长度
) -> *mut DataFrameContext {
    let ctx = unsafe { &mut *df_ptr };
    let df = &ctx.df;

    // 1. 将 C 数组转换为 Rust Vec<Expr>
    // 注意：这里我们不拥有指针数组本身，但我们要“偷走”数组里每个指针指向的 Expr
    let mut exprs = Vec::with_capacity(len);
    unsafe {
        // 构建一个临时的 slice 来遍历
        let ptr_slice = std::slice::from_raw_parts(exprs_ptr, len);
        for &ptr in ptr_slice {
            // Box::from_raw 会拿回所有权，这意味着 C# 那边的 Handle 实际上"空"了
            // 这通常符合 Select 的语义（消耗掉表达式）
            let expr_ctx = Box::from_raw(ptr);
            exprs.push(expr_ctx.inner);
        }
    }

    println!("(Rust) Selecting {} columns...", exprs.len());

    // 2. 执行 Select
    let lf = df.clone().lazy();
    let new_lf = lf.select(exprs); // select 接受 Vec<Expr>

    match new_lf.collect() {
        Ok(res_df) => {
            println!("(Rust) Select done. Cols: {}", res_df.width());
            Box::into_raw(Box::new(DataFrameContext { df: res_df }))
        },
        Err(e) => {
            println!("(Rust) Select failed: {}", e);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_filter(
    df_ptr: *mut DataFrameContext, 
    expr_ptr: *mut ExprContext
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &mut *df_ptr };
        
        // 1. 拿回 Expr 所有权 (Consume)
        let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
        let expr = expr_ctx.inner;

        println!("(Rust) Executing generic filter...");

        // 2. 执行计算
        // 使用 ? 操作符，如果出错直接跳出宏，写入 LAST_ERROR
        let res_df = ctx.df.clone()
            .lazy()
            .filter(expr)
            .collect()?; 

        println!("(Rust) Filter success. Rows: {}", res_df.height());

        // 3. 成功返回 (必须包裹在 Ok 里)
        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}


// ==========================================
// GroupBy 核心逻辑
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_groupby_agg(
    df_ptr: *mut DataFrameContext,
    // 分组依据列 (例如: ["name"])
    by_ptr: *const *mut ExprContext,
    by_len: usize,
    // 聚合表达式 (例如: [sum(age), mean(age)])
    agg_ptr: *const *mut ExprContext,
    agg_len: usize
) -> *mut DataFrameContext {
    let ctx = unsafe { &mut *df_ptr };
    let df = &ctx.df;

    // 1. 提取 "By" 表达式列表
    let mut by_exprs = Vec::with_capacity(by_len);
    unsafe {
        for &ptr in std::slice::from_raw_parts(by_ptr, by_len) {
            by_exprs.push(Box::from_raw(ptr).inner);
        }
    }

    // 2. 提取 "Agg" 表达式列表
    let mut agg_exprs = Vec::with_capacity(agg_len);
    unsafe {
        for &ptr in std::slice::from_raw_parts(agg_ptr, agg_len) {
            agg_exprs.push(Box::from_raw(ptr).inner);
        }
    }

    println!("(Rust) GroupBy {} cols, Aggregating {} exprs...", by_exprs.len(), agg_exprs.len());

    // 3. 执行 GroupBy -> Agg -> Collect
    // 这里的链式调用是 Polars 的精髓
    let lf = df.clone().lazy();
    let new_lf = lf.group_by(by_exprs).agg(agg_exprs);

    match new_lf.collect() {
        Ok(res_df) => {
            println!("(Rust) GroupBy done. Result shape: {:?}", res_df.shape());
            Box::into_raw(Box::new(DataFrameContext { df: res_df }))
        },
        Err(e) => {
            println!("(Rust) GroupBy failed: {}", e);
            std::ptr::null_mut()
        }
    }
}

// ==========================================
// Join (连接)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_join(
    left_ptr: *mut DataFrameContext,
    right_ptr: *mut DataFrameContext,
    // 左表 Join Key (如 col("id"))
    left_on_ptr: *const *mut ExprContext,
    left_on_len: usize,
    // 右表 Join Key (如 col("user_id"))
    right_on_ptr: *const *mut ExprContext,
    right_on_len: usize,
    // Join 类型 ("left", "inner", "outer")
    how_ptr: *const c_char
) -> *mut DataFrameContext {
    let left_ctx = unsafe { &*left_ptr };
    let right_ctx = unsafe { &*right_ptr };
    
    // 1. 解析 Join 类型
    let how_str = ptr_to_str(how_ptr).unwrap();
    let how = match how_str {
        "inner" => JoinType::Inner,
        "left" => JoinType::Left,
        "outer" | "full" => JoinType::Full,
        "cross" => JoinType::Cross,
        _ => JoinType::Inner, // 默认 Inner
    };

    // 2. 提取表达式 (Helper Closure)
    let get_exprs = |ptr: *const *mut ExprContext, len: usize| unsafe {
        let mut exprs = Vec::with_capacity(len);
        for &p in std::slice::from_raw_parts(ptr, len) {
            // Box::from_raw 拿回所有权
            exprs.push(Box::from_raw(p).inner);
        }
        exprs
    };

    let left_on = get_exprs(left_on_ptr, left_on_len);
    let right_on = get_exprs(right_on_ptr, right_on_len);

    println!("(Rust) Joining: {} on {:?} == {:?}", how_str, left_on, right_on);

    // 3. 转换为 LazyFrame 并执行 Join
    let lf_left = left_ctx.df.clone().lazy();
    let lf_right = right_ctx.df.clone().lazy();

    // 构建 JoinArgs (Polars 0.50 写法)
    let args = JoinArgs::new(how);

    // 执行 Join
    let new_lf = lf_left.join(lf_right, left_on, right_on, args);

    match new_lf.collect() {
        Ok(res_df) => {
            println!("(Rust) Join success. Shape: {:?}", res_df.shape());
            Box::into_raw(Box::new(DataFrameContext { df: res_df }))
        },
        Err(e) => {
            println!("(Rust) Join failed: {}", e);
            std::ptr::null_mut()
        }
    }
}