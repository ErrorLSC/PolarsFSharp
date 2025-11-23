use std::ffi::CStr;
use std::os::raw::c_char;
use polars::prelude::*;
use polars_core::prelude::CompatLevel;
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_arrow::ffi::{ArrowArray, ArrowSchema, export_array_to_c, export_field_to_c};
use polars_io::prelude::{ParquetReader, ParquetWriter};
use polars_io::{SerReader, SerWriter};
use std::ops::Mul;
use std::fs::File;
use std::cell::RefCell;
use std::panic::catch_unwind;
use std::panic::AssertUnwindSafe;

// ==========================================
// 0. 错误处理基础设施
// ==========================================

// 线程局部存储，用来存放上一次的错误信息
thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = RefCell::new(None);
}

// 供 C# 调用的函数：获取并清空错误信息
#[unsafe(no_mangle)]
pub extern "C" fn pl_get_last_error() -> *mut c_char {
    let msg = LAST_ERROR.with(|e| e.borrow_mut().take());
    match msg {
        Some(s) => std::ffi::CString::new(s).unwrap().into_raw(),
        None => std::ptr::null_mut(),
    }
}

// 供 C# 调用的函数：释放错误信息字符串内存
#[unsafe(no_mangle)]
pub extern "C" fn pl_free_error_msg(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe { let _ = std::ffi::CString::from_raw(ptr); }
    }
}

// 辅助函数：设置错误信息
fn set_error(msg: String) {
    LAST_ERROR.with(|e| *e.borrow_mut() = Some(msg));
}

// --- 核心宏 ---
// 这个宏做了两件事：
// 1. 捕获 Panic (catch_unwind)
// 2. 处理 PolarsResult (Ok/Err)
// 如果失败，设置错误信息并返回 null
macro_rules! ffi_try {
    ($body:expr) => {{
        // 1. 定义一个闭包，强制它返回 PolarsResult
        // 这样我们在 $body 里面就可以愉快地使用 ? 操作符了
        let closure = || -> PolarsResult<_> { $body };

        // 2. 捕获 Panic
        let result = catch_unwind(AssertUnwindSafe(closure));

        match result {
            // 没有 Panic
            Ok(inner_result) => match inner_result {
                // 业务逻辑成功 -> 返回值
                Ok(val) => val,
                // 业务逻辑报错 (比如 ? 抛出的 PolarsError) -> 记录错误并返回 null
                Err(e) => {
                    set_error(e.to_string());
                    std::ptr::null_mut()
                }
            },
            // 发生了 Panic
            Err(e) => {
                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown Rust Panic".to_string()
                };
                set_error(msg);
                std::ptr::null_mut()
            }
        }
    }};
}

// ==========================================
// 1. 定义“不透明”容器
// ==========================================
// 这是一个壳，专门用来在 C# 和 Rust 之间传递 DataFrame 的所有权
pub struct DataFrameContext {
    pub df: DataFrame,
}
// 定义 Expr 的壳子
pub struct ExprContext {
    pub inner: Expr,
}
// 辅助函数
fn ptr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, std::str::Utf8Error> {
    if ptr.is_null() { panic!("Null pointer"); }
    unsafe { CStr::from_ptr(ptr).to_str() }
}
// 定义 LazyFrame 壳子
pub struct LazyFrameContext {
    pub inner: LazyFrame,
}

// ==========================================
// 2. 核心 API
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_read_csv(path_ptr: *const c_char, 
    try_parse_dates: bool
) -> *mut DataFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr).unwrap();

        let mut options = CsvReadOptions::default();
        
        // 使用 Arc::make_mut 获取可变引用
        // 这句话的意思是："我要修改 parse_options 里的东西，帮我搞个可写的引用来"
        std::sync::Arc::make_mut(&mut options.parse_options).try_parse_dates = try_parse_dates;

        // 读取数据，但 *不* 转换成 Arrow
        let df = options
            .try_into_reader_with_file_path(Some(path.into()))
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))? // 处理可能的 io error
            .finish()?;

        // 把 DataFrame 装箱，放到堆内存上，并返回指针
        // Box::into_raw 告诉 Rust："我放弃管理这块内存，你把地址给我，别自动回收"
        let context = Box::new(DataFrameContext { df });
        Ok(Box::into_raw(context))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_free_dataframe(ptr: *mut DataFrameContext) {
    if !ptr.is_null() {
        // 拿回所有权，离开作用域时自动 Drop (释放内存)
        unsafe { let _ = Box::from_raw(ptr); }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_to_arrow(
    ctx_ptr: *mut DataFrameContext, 
    out_chunk: *mut ArrowArray, 
    out_schema: *mut ArrowSchema
) {
    // 借用 DataFrame (不要拿走所有权，因为 C# 还要继续用)
    let ctx = unsafe { &mut *ctx_ptr };
    let df = &mut ctx.df;

    // --- 下面是之前的转换逻辑 (完全复用) ---
    let columns = df.get_columns()
        .iter()
        .map(|s| s.clone().rechunk_to_arrow(CompatLevel::newest()))
        .collect::<Vec<_>>();

    let arrow_schema = df.schema().to_arrow(CompatLevel::newest());
    let fields: Vec<Field> = arrow_schema.iter_values().cloned().collect();

    let struct_array = StructArray::new(
        ArrowDataType::Struct(fields.clone()), 
        df.height(),
        columns,
        None
    );

    unsafe {
        *out_chunk = export_array_to_c(Box::new(struct_array));
        let root_field = Field::new("".into(), ArrowDataType::Struct(fields), false);
        *out_schema = export_field_to_c(&root_field);
    }
}
// ==========================================
// Scan (入口：从文件创建 LazyFrame)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_csv(path_ptr: *const c_char,
    try_parse_dates: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr).unwrap();
        
        // 使用方案 A (PathBuf) 或方案 C (String)，取决于你刚才哪个跑通了
        // 这里假设是用 PathBuf + cloud 关闭，或者 Path + cloud 开启 + new()
        // 无论哪种，重点是最后一行：
        
        let lf = LazyCsvReader::new(PlPath::new(path))
            .with_try_parse_dates(try_parse_dates)
            .finish()?; // ? 使用合法

        // [重要修改] 必须包裹在 Ok() 里
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_parquet(path_ptr: *const c_char) -> *mut LazyFrameContext {
    let path = ptr_to_str(path_ptr).unwrap();
    println!("(Rust) Scanning Parquet (Lazy): {}", path);

    // ScanArgs 可以配置很多优化，这里用默认
    let args = ScanArgsParquet::default();
    
    let lf = LazyFrame::scan_parquet(PlPath::new(path), args).unwrap();

    Box::into_raw(Box::new(LazyFrameContext { inner: lf }))
}

// ==========================================
// Lazy Transformations (中间过程)
// ==========================================

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
// 新增聚合表达式 (Aggregations)
// ==========================================

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

// ==========================================
// 输出 (Sink / Write)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_write_csv(df_ptr: *mut DataFrameContext, path_ptr: *const c_char) {
    let ctx = unsafe { &mut *df_ptr };
    let path = ptr_to_str(path_ptr).unwrap();

    println!("(Rust) Saving to CSV: {}", path);

    let mut file = File::create(path).expect("Could not create file");

    CsvWriter::new(&mut file)
        .finish(&mut ctx.df)
        .unwrap();
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_write_parquet(df_ptr: *mut DataFrameContext, path_ptr: *const c_char) {
    let ctx = unsafe { &mut *df_ptr };
    let path = ptr_to_str(path_ptr).unwrap();

    println!("(Rust) Saving to Parquet: {}", path);

    let file = File::create(path).expect("Could not create file");

    // ParquetWriter 在 0.50 的用法
    ParquetWriter::new(file)
        .finish(&mut ctx.df)
        .unwrap();
}

// ==========================================
// 读取 Parquet
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_parquet(path_ptr: *const c_char) -> *mut DataFrameContext {
    let path = ptr_to_str(path_ptr).unwrap();

    let file = File::open(path).expect("File not found");

    let df = ParquetReader::new(file)
        .finish()
        .unwrap();

    Box::into_raw(Box::new(DataFrameContext { df }))
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