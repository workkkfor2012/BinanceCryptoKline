//! 性能分析宏
//! 
//! 提供 `perf_profile` 宏，用于为函数添加性能分析功能。
//! 该宏会自动注入 `target = "perf"` 到 tracing::instrument 中，
//! 实现与业务日志的完全隔离。

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse::Parser, punctuated::Punctuated, Meta, Token};

/// 性能分析宏
/// 
/// 用法：
/// ```rust
/// #[perf_profile(skip_all, fields(symbol = %task.symbol))]
/// async fn my_function() {
///     // 函数实现
/// }
/// ```
/// 
/// 该宏会自动将 `target = "perf"` 注入到 tracing::instrument 中，
/// 确保性能日志与业务日志完全隔离。
#[proc_macro_attribute]
pub fn perf_profile(attr: TokenStream, item: TokenStream) -> TokenStream {
    // 1. 解析传递给宏的属性，例如 `skip_all, fields(id=1)`
    let attr_args = if attr.is_empty() {
        // 如果没有传递属性，创建一个空的列表
        Punctuated::<Meta, Token![,]>::new()
    } else {
        Punctuated::<Meta, Token![,]>::parse_terminated
            .parse(attr)
            .expect("Failed to parse perf_profile attributes")
    };

    // 2. 解析函数本身
    let func: syn::ItemFn = syn::parse(item)
        .expect("perf_profile must be applied to a function");

    // 3. 直接修改函数，添加 tracing::instrument 属性
    //    核心：将 target = "perf" 与用户传入的属性结合起来
    let new_func = if attr_args.is_empty() {
        // 没有额外参数，只添加 target = "perf"
        quote! {
            #[tracing::instrument(target = "perf")]
            #func
        }
    } else {
        // 有额外参数，将它们与 target = "perf" 组合
        quote! {
            #[tracing::instrument(target = "perf", #attr_args)]
            #func
        }
    };

    // 4. 返回修改后的函数
    new_func.into()
}
