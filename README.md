# PolarsFSharp

A high-performance F# DataFrame library powered by Polars (Rust) and Apache Arrow.

## Features

- **Zero-Copy Interop**: Uses Arrow C Data Interface for efficient data transfer.
- **Lazy Execution**: Query optimization with predicate pushdown.
- **Type Safety**: F# DSL with SRTP for elegant query building.
- **Hybrid IO**: Supports Local and Cloud paths (S3/Azure) via Polars 0.50.

## Architecture

- **Core**: Rust () linking to  & .
- **Bridge**: C# () handling P/Invoke & SafeHandles.
- **API**: F# () providing the functional DSL.

## Usage

```fsharp
open PolarsFSharp

let df = 
    Polars.scanCsv "data.csv" None
    |> Polars.filter (Polars.col "age" .> Polars.lit 18)
    |> Polars.collect

df |> Polars.show
```
