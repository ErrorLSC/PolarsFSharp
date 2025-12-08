namespace Polars.FSharp

open System
open Apache.Arrow
open Apache.Arrow.Types

module Udf =

    // ==========================================
    // 1. 内部工厂：Builder 创建器
    // ==========================================
    // 职责：根据目标 DataType 创建对应的 Arrow Builder
    // 返回：(写入函数: obj option -> unit, 构建函数: unit -> IArrowArray)
    let private createBuilder (dtype: DataType) (capacity: int) : (obj option -> unit) * (unit -> IArrowArray) =
        match dtype with
        | DataType.Int32 ->
            // Reserve(capacity) 是性能关键，防止多次扩容
            let b = (new Int32Array.Builder()).Reserve capacity
            let append (v: obj option) =
                match v with
                | Some x -> b.Append(unbox<int> x) |> ignore
                | None -> b.AppendNull() |> ignore
            append, fun () -> b.Build() :> IArrowArray
            
        | DataType.Int64 ->
            let b = (new Int64Array.Builder()).Reserve capacity
            let append (v: obj option) =
                match v with
                | Some x -> b.Append(unbox<int64> x) |> ignore
                | None -> b.AppendNull() |> ignore
            append, fun () -> b.Build() :> IArrowArray

        | DataType.Float64 ->
            let b = (new DoubleArray.Builder()).Reserve capacity
            let append (v: obj option) =
                match v with
                | Some x -> b.Append(unbox<double> x) |> ignore
                | None -> b.AppendNull() |> ignore
            append, fun () -> b.Build() :> IArrowArray

        | DataType.String ->
            // Polars 0.50+ 推荐使用 StringViewArray
            let b = (new StringViewArray.Builder()).Reserve capacity
            let append (v: obj option) =
                match v with
                | Some x -> b.Append(unbox<string> x) |> ignore
                | None -> b.AppendNull() |> ignore
            append, fun () -> b.Build() :> IArrowArray
            
        | DataType.Boolean ->
            let b = (new BooleanArray.Builder()).Reserve capacity
            let append (v: obj option) =
                match v with
                | Some x -> b.Append(unbox<bool> x) |> ignore
                | None -> b.AppendNull() |> ignore
            append, fun () -> b.Build() :> IArrowArray

        | _ -> failwithf "UDF output type %A not supported yet" dtype

    // ==========================================
    // 2. 内部工厂：Reader 创建器
    // ==========================================
    // 职责：根据输入 Array 类型和泛型 'T，生成读取闭包
    // 返回：int -> 'T option
    let private createReader<'T> (arr: IArrowArray) : int -> 'T option =
        // 使用 box Unchecked.defaultof<'T> 来进行泛型类型匹配
        match box Unchecked.defaultof<'T> with
        | :? int -> 
            // 处理输入是 Int32 或 Int64 但用户想要 int 的情况
            match arr with
            | :? Int32Array as a -> 
                fun i -> if a.IsNull(i) then None else Some (unbox<'T> (a.GetValue(i).Value))
            | :? Int64Array as a -> 
                // 注意：这里可能发生截断，但通常 F# int 是 int32
                fun i -> if a.IsNull(i) then None else Some (unbox<'T> (int (a.GetValue(i).Value)))
            | _ -> failwithf "Cannot read %s as int" (arr.GetType().Name)

        | :? int64 -> 
            match arr with
            | :? Int64Array as a -> 
                fun i -> if a.IsNull(i) then None else Some (unbox<'T> (a.GetValue(i).Value))
            | :? Int32Array as a -> 
                fun i -> if a.IsNull(i) then None else Some (unbox<'T> (int64 (a.GetValue(i).Value)))
            | _ -> failwithf "Cannot read %s as int64" (arr.GetType().Name)

        | :? double -> 
            match arr with
            | :? DoubleArray as a -> 
                fun i -> if a.IsNull(i) then None else Some (unbox<'T> (a.GetValue(i).Value))
            | :? FloatArray as a ->
                fun i -> if a.IsNull(i) then None else Some (unbox<'T> (double (a.GetValue(i).Value)))
            | _ -> failwithf "Cannot read %s as double" (arr.GetType().Name)

        | :? string ->
            match arr with
            | :? StringArray as sa -> 
                fun i -> if sa.IsNull(i) then None else Some (unbox<'T> (sa.GetString(i)))
            | :? StringViewArray as sva ->
                fun i -> if sva.IsNull(i) then None else Some (unbox<'T> (sva.GetString(i)))
            | _ -> failwithf "Cannot read %s as string" (arr.GetType().Name)
            
        | :? bool ->
            match arr with
            | :? BooleanArray as a ->
                 fun i -> if a.IsNull i then None else Some (unbox<'T> (a.GetValue(i).Value))
            | _ -> failwithf "Cannot read %s as bool" (arr.GetType().Name)

        | _ -> failwithf "Unsupported UDF input type: %s" typeof<'T>.Name

    // ==========================================
    // 3. 公开 API：Option 版本的 Map
    // ==========================================
    // 这是核心函数，处理所有 Null 逻辑
    let mapOption<'T, 'U> (f: 'T option -> 'U option) (outputType: DataType) : Func<IArrowArray, IArrowArray> =
        Func<IArrowArray, IArrowArray>(fun arr ->
            let len = arr.Length
            
            // 1. 准备 Writer (带预分配)
            let append, build = createBuilder outputType len

            // 2. 准备 Reader (已内联类型检查)
            let getter = createReader<'T> arr

            // 3. 循环执行
            for i in 0 .. len - 1 do
                let input = getter i
                let output = f input // 执行用户逻辑
                
                // 将 'U option 转为 obj option 以便通用 Writer 处理
                let outObj = output |> Option.map box
                append outObj

            // 4. 构建结果
            build()
        )

    // ==========================================
    // 4. 公开 API：普通版本的 Map
    // ==========================================
    // 方便用户处理非 Null 逻辑，自动处理 Some/None 包装
    let map (f: 'T -> 'U) (outputType: DataType) =
        mapOption (fun (opt: 'T option) ->
            match opt with
            | Some v -> Some (f v)
            | None -> None
        ) outputType