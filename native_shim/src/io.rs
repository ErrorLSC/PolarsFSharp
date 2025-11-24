use polars::prelude::*;
use polars_arrow::ffi::{ArrowArray, ArrowSchema, export_array_to_c, export_field_to_c};
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_core::prelude::CompatLevel;
use std::os::raw::c_char;
use std::fs::File;
use crate::types::{DataFrameContext, LazyFrameContext, ptr_to_str};

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
    ffi_try_void!({
        if !ptr.is_null() {
        // 拿回所有权，离开作用域时自动 Drop (释放内存)
        unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
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
