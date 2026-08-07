/*
    Copyright 2021 Sojan James

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

// Rust deserializer for CycloneDDS. (proc macro)
// See discussion at https://github.com/eclipse-cyclonedds/cyclonedds/issues/830

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::ext::IdentExt;
use syn::{Field, Ident};

/// Topic deriveの生成コードが非修飾で参照するcyclonedds-rs側の識別子。
/// Why not 完全修飾: これらは`use cyclonedds_rs::*`前提の既存規約で参照しており、
/// 絶対パス化には`extern crate self as cyclonedds_rs`等の追加機構が必要になる。
/// ROS型名(CamelCase)と衝突する現実性が低いため、同名structへのderiveを事前拒否する。
const TOPIC_RESERVED_IDENTS: &[&str] = &[
    "TopicType",
    "FixedTopicType",
    "SampleBuffer",
    "DdsParticipant",
    "DdsQos",
    "DdsListener",
    "DdsTopic",
    "DDSError",
];

/// DdsInterface deriveの生成コードが非修飾で参照するcyclonedds-rs側の識別子
const DDS_INTERFACE_RESERVED_IDENTS: &[&str] = &[
    "InterfaceRef",
    "InterfaceDef",
    "InterfaceElem",
    "NsMode",
    "DefRegistry",
    "MsgDefRegistry",
];

/// derive対象のstructがジェネリクス/ライフタイムパラメータを持たないか検査する。
/// 生成コードは`impl Trait for #struct_ident`のようにジェネリクスを引き継がずに書いているため、
/// ジェネリクス付きstructをderiveするとE0425(型が見つからない)/E0107(ジェネリクス不足)等の
/// 生成コード内部事情のエラーになる。ジェネリクス対応は行わず、入り口で明示的に拒否する
/// (ジェネリックキーをE0412から明確なエラーに変えた既存判断と同じ方向)。
/// `where`節のみ(params空)は実害がないため検査対象外でよい。
fn check_no_generics(derive_name: &str, item: &syn::ItemStruct) -> Result<(), syn::Error> {
    if item.generics.params.is_empty() {
        Ok(())
    } else {
        Err(syn::Error::new_spanned(
            &item.generics,
            format!("#[derive({derive_name})]: generic or lifetime parameters are not supported"),
        ))
    }
}

/// derive対象のstruct名が予約識別子と衝突していないか検査する。
/// 衝突すると生成コード自身がstruct名を予約識別子として誤解決して自壊するため、
/// 分かりやすいエラーで事前に拒否する。
fn check_reserved_collision(
    derive_name: &str,
    item: &syn::ItemStruct,
    reserved: &[&str],
) -> Result<(), syn::Error> {
    let name = item.ident.unraw().to_string();
    if reserved.contains(&name.as_str()) {
        Err(syn::Error::new_spanned(
            &item.ident,
            format!(
                "#[derive({derive_name})]: struct name `{name}` collides with a cyclonedds-rs identifier used by the generated code; rename the Rust struct (for DdsInterface, keep the wire name with #[cdds(name = \"{name}\")])"
            ),
        ))
    } else {
        Ok(())
    }
}

/// TopicTypeを実装するderiveマクロ
///
/// # Attributes
/// * `cdds(fixed_size)` - トピックが固定長であることを示す。Iceoryx転送でCDRシリアライズされなくなる
/// * `cdds(package = "custom_msg")` - トピックの型名(ワイヤ型名)の元になるパッケージ名。
///   `#[derive(DdsInterface)]`と共通の属性であり、両方をderiveした場合でも唯一の情報源になる
///   （`typename()`は`"{package}/{name}"`として生成される）
/// * `cdds(name = "CustomTypeName")` - 型名の上書き。省略時はRust側のstruct名を使う
///
/// # Field Attributes
/// * `topic_key` - フィールドをCDRのキーとして扱うことを示す
/// * `topic_key_enum` - フィールドがキーであり、かつ列挙型であることを示す（列挙型はプリミティブとして扱う）
#[proc_macro_derive(Topic, attributes(cdds, topic_key, topic_key_enum))]
pub fn derive_topic(item: TokenStream) -> TokenStream {
    derive_topic_impl(item.into()).into()
}

fn derive_topic_impl(item: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    let topic_struct = match syn::parse2::<syn::ItemStruct>(item) {
        Ok(item) => item,
        Err(err) => return err.to_compile_error(),
    };

    if let Err(err) = check_no_generics("Topic", &topic_struct) {
        return err.to_compile_error();
    }

    if let Err(err) = check_reserved_collision("Topic", &topic_struct, TOPIC_RESERVED_IDENTS) {
        return err.to_compile_error();
    }

    let mut ts = match build_key_holder_struct(&topic_struct) {
        Ok(ts) => ts,
        Err(err) => return err.to_compile_error(),
    };
    let ts2 = create_keyhash_functions(&topic_struct);
    let ts3 = create_topic_functions(&topic_struct);

    ts.extend(ts2);
    ts.extend(ts3);

    //println!("KEYHOLDER:{:?}",ts.clone().to_string());
    ts
}

///Create a key holder struct from the given struct. The key
///fields will be included in this structure. The structure
///will be empty if there are no key fields.
fn build_key_holder_struct(item: &syn::ItemStruct) -> Result<proc_macro2::TokenStream, syn::Error> {
    let key_holder_struct = item;

    let mut holder_name = key_holder_struct.ident.to_string();
    let fields = &key_holder_struct.fields;
    holder_name.push_str("KeyHolder_");
    let holder_name = Ident::new(&holder_name, Span::call_site());
    //key_holder_struct.ident = Ident::new(&holder_name,Span::call_site());

    let mut field_idents = Vec::new();
    let mut field_types = Vec::new();
    let mut clone_or_into = Vec::new();
    let mut ref_or_value = Vec::new();
    let mut contained_types = Vec::new();
    let mut variable_length = false;

    for field in fields {
        if is_key(field) {
            // キー型の検査はvalidate_key_field_typeに集約している。
            // ここに到達したフィールドは名前付き かつ 表現可能なキー型であることが保証される
            // ので、以降の構築ロジックはその前提でunwrap/型分岐を行う。
            validate_key_field_type(field)?;
            field_idents.push(field.ident.as_ref().unwrap().clone());
            if is_primitive(field) || is_key_enum(field) {
                field_types.push(field.ty.clone());
                clone_or_into.push(quote! {clone()});
                ref_or_value.push(quote! {});
                if !variable_length {
                    variable_length = is_variable_length(field);
                }
            } else {
                match field.ty.clone() {
                    syn::Type::Path(mut type_path) => {
                        // if the key is another structure (not a primitive),
                        // there should be a key holder structure for it.
                        // Change the type
                        let last_segment = type_path.path.segments.last_mut().unwrap();
                        let mut ident_string = last_segment.ident.to_string();
                        ident_string.push_str("KeyHolder_");
                        let new_ident = Ident::new(&ident_string, Span::call_site());
                        //replace the ident with the new name
                        last_segment.ident = new_ident;
                        contained_types.push(syn::Type::Path(type_path.clone()));
                        field_types.push(syn::Type::Path(type_path));
                        clone_or_into.push(quote! {into()});
                        ref_or_value.push(quote! { &});
                    }
                    syn::Type::Array(type_arr) => {
                        field_types.push(field.ty.clone());
                        clone_or_into.push(quote! {clone()});
                        ref_or_value.push(quote! {});
                        // `[String; N]`は要素が可変長(String)なので、keyhashが
                        // 16 byteを超え得る場合にRTPS仕様上必須のMD5 keyhashを使う判定
                        // (force_md5_keyhash())の材料になるvariable_lengthをtrueにする必要がある。
                        // 以前はプリミティブ単独キーのブランチしかvariable_lengthを更新しておらず、
                        // 配列キーは常にfalse(is_variable_lengthがType::Pathしか見ないため)だった。
                        if !variable_length
                            && let syn::Type::Path(elem_path) = &*type_arr.elem
                            && is_std_path(&elem_path.path, "string", "String")
                        {
                            variable_length = true;
                        }
                    }
                    // validate_key_field_typeがPath/Array以外のキー型を既に拒否している
                    _ => unreachable!(
                        "validate_key_field_type should reject non path/array key types"
                    ),
                }
            }
        }
    }

    let item_ident = &item.ident;
    //println!("Filtered fields:{:?}", &filtered_fields);

    // KeyHolder_structは元struct(item)と同じ可視性を引き継ぐ。以前は常に非pub
    // だったため、別モジュールのpub structをキーにすると生成コードが参照する
    // `PointKeyHolder_`がE0603(privateなstructの外部参照)になっていた。
    // #[doc(hidden)]も付けて、内部実装用の型であることを明示し公開API面への露出を抑える。
    let vis = &item.vis;

    // Why ::std完全修飾: proc-macro出力は呼び出し側の名前解決に従うため、呼び出し側の
    // 同名型シャドー(std_msgs/String等)に生成コードが影響されないよう絶対パスで参照する。
    // derive属性(Default等)はマクロ名前空間で解決されるため型シャドーの影響を受けない
    let ts = quote! {
        #[derive(Default, Deserialize, Serialize, PartialEq, Clone)]
        #[doc(hidden)]
        #vis struct #holder_name {
            #(#field_idents:#field_types,)*
        }

        impl ::std::convert::From<& #item_ident> for #holder_name {
            fn from(source: & #item_ident) -> Self {
                Self {
                    #(#field_idents : (#ref_or_value source.#field_idents). #clone_or_into ,)*
                }
            }
        }

        impl #holder_name {
            // 別モジュールのKeyHolder_(struct自体は#visで可視性を引き継いだ)を
            // ネストキーとして参照する場合、この関連関数も同じ可視性が必要
            // (非pubのままだと`OuterKeyHolder_::is_variable_length()`内の
            // `InnerKeyHolder_::is_variable_length()`呼び出しがE0624になる)
            #vis const fn is_variable_length() -> bool {
                if !#variable_length {
                    #(#contained_types :: is_variable_length()||)*  false
                } else {
                    true
                }
            }
        }

    };

    Ok(ts)
}

/// `#[topic_key]`が付いたフィールドの型がキーとして表現可能かを検査する。
/// 許可される型: 非ジェネリックの単一パス(プリミティブ or `Topic`をderiveした構造体)、
/// または要素がプリミティブの`[T; N]`配列のみ。
///
/// Why共通化: `Topic` deriveのkeyhash構築(`build_key_holder_struct`)と、
/// `DdsInterface` deriveの`@key`前置で同じ判定・同じエラー文言を使う
fn validate_key_field_type(field: &Field) -> Result<(), syn::Error> {
    // タプルstructのフィールドはidentを持たず、KeyHolder_のフィールド名にできない。
    // 以前は`field.ident.unwrap()`がproc-macro panicになっていたため、明確なエラーで拒否する。
    if field.ident.is_none() {
        return Err(syn::Error::new_spanned(
            field,
            "#[topic_key]: tuple struct fields cannot be keys; use a struct with named fields",
        ));
    }

    // usize/isize/u128/i128はis_primitive_type_pathでプリミティブ扱いだが、
    // usize/isizeは幅がプラットフォーム依存でワイヤ互換性がなく、u128/i128はcdrクレートが
    // シリアライズできず実行時エラーになる。is_primitive_type_pathから外すと「構造体扱い→
    // XKeyHolder_が見つからない」という難解なエラーに化けるため、キー分類の前段(単独パス・
    // 配列要素の両方)で明示的に拒否する。
    const UNPORTABLE_INT_MSG: &str = "#[topic_key]: `usize`/`isize`/`u128`/`i128` have no portable CDR representation; use a fixed-width type (u8..u64, i8..i64)";
    if let syn::Type::Path(type_path) = &field.ty
        && is_unportable_int_path(type_path)
    {
        return Err(syn::Error::new_spanned(&field.ty, UNPORTABLE_INT_MSG));
    }
    if let syn::Type::Array(type_arr) = &field.ty
        && let syn::Type::Path(array_type_path) = &*type_arr.elem
        && is_unportable_int_path(array_type_path)
    {
        return Err(syn::Error::new_spanned(&field.ty, UNPORTABLE_INT_MSG));
    }

    // プリミティブ(完全修飾Stringを含む)は無条件で許可
    if is_primitive(field) {
        return Ok(());
    }
    // キー列挙型は「プリミティブとして扱う」マーカーであり、型の形自体の検証(非ジェネリック
    // 単一パス)は免除しない。免除するとVec<T>やOption<T>等も無検証で通ってしまう。
    if is_key_enum(field) {
        if let syn::Type::Path(type_path) = &field.ty {
            let last_segment = type_path.path.segments.last().unwrap();
            if matches!(last_segment.arguments, syn::PathArguments::None) {
                return Ok(());
            }
        }
        return Err(syn::Error::new_spanned(
            &field.ty,
            "#[topic_key_enum]: enum keys must be a non-generic single-path type",
        ));
    }

    match &field.ty {
        syn::Type::Path(type_path) => {
            // if the key is another structure (not a primitive),
            // there should be a key holder structure for it.
            let last_segment = type_path.path.segments.last().unwrap();
            // Why: Arc<T>/Vec<T>等のジェネリック型は「TKeyHolder_を参照する」規約に
            // 乗せられず、存在しない型名を指す難解なエラー(E0412)になるため、
            // フィールド型を指す明確なエラーで事前拒否する
            if !matches!(last_segment.arguments, syn::PathArguments::None) {
                return Err(syn::Error::new_spanned(
                    &field.ty,
                    "#[topic_key]: generic type is not supported as a key; keys must be primitives, [primitive; N] arrays, or structs deriving Topic",
                ));
            }
            Ok(())
        }
        syn::Type::Array(type_arr) => {
            if let syn::Type::Path(array_type_path) = &*type_arr.elem {
                if is_primitive_type_path(array_type_path) {
                    Ok(())
                } else {
                    Err(syn::Error::new_spanned(
                        &field.ty,
                        "#[topic_key]: only arrays of primitives are supported as keys",
                    ))
                }
            } else {
                Err(syn::Error::new_spanned(
                    &field.ty,
                    "#[topic_key]: unsupported array element type for a key",
                ))
            }
        }
        _ => Err(syn::Error::new_spanned(
            &field.ty,
            "#[topic_key]: keys must be primitives, [primitive; N] arrays, or structs deriving Topic",
        )),
    }
}

/// pathが`usize`/`isize`/`u128`/`i128`のいずれかを指すか判定する
fn is_unportable_int_path(type_path: &syn::TypePath) -> bool {
    type_path.path.is_ident("usize")
        || type_path.path.is_ident("isize")
        || type_path.path.is_ident("u128")
        || type_path.path.is_ident("i128")
}

/// `[a-z][a-z0-9_]*` かつ `__` を含まず `_` で終わらない、という共通の命名規則の判定本体。
/// ROS 2 の.msgフィールド名規則とROS 2パッケージ名規則REP 144は同じ形をしているため、
/// 判定ロジックを共有し、呼び出し側で対象に応じたエラー文言を付ける。
/// Why手書き判定: 正規表現クレートを追加するとdds_derive(proc-macroクレート)の依存が増える。
/// ASCII判定だけで規則を表現できるためcharメソッドで十分。
fn validate_lower_snake_case(s: &str) -> Result<(), String> {
    let mut chars = s.chars();
    let first = match chars.next() {
        Some(c) => c,
        None => return Err("must not be empty".to_string()),
    };
    if !first.is_ascii_lowercase() {
        return Err(format!(
            "must start with a lowercase ASCII letter, found `{first}`"
        ));
    }
    let mut prev_underscore = false;
    for c in chars {
        if c == '_' {
            if prev_underscore {
                return Err("must not contain consecutive underscores (`__`)".to_string());
            }
            prev_underscore = true;
        } else if c.is_ascii_lowercase() || c.is_ascii_digit() {
            prev_underscore = false;
        } else {
            return Err(format!(
                "must contain only lowercase ASCII letters, digits, or `_`, found `{c}`"
            ));
        }
    }
    if s.ends_with('_') {
        return Err("must not end with an underscore".to_string());
    }
    Ok(())
}

/// OMG IDL 4.2 Table 7-6の全キーワード(小文字化済み)。
/// フィールド名は`validate_lower_snake_case`で小文字強制されるため、`TRUE`/`FALSE`/`Object`/
/// `ValueBase`のような大文字始まりのキーワードも小文字形で比較すれば十分。
const IDL_KEYWORDS: &[&str] = &[
    "abstract",
    "any",
    "alias",
    "attribute",
    "bitfield",
    "bitmask",
    "bitset",
    "boolean",
    "case",
    "char",
    "component",
    "connector",
    "const",
    "consumes",
    "context",
    "custom",
    "default",
    "double",
    "exception",
    "emits",
    "enum",
    "eventtype",
    "factory",
    "false",
    "finder",
    "fixed",
    "float",
    "getraises",
    "home",
    "import",
    "in",
    "inout",
    "interface",
    "local",
    "long",
    "manages",
    "map",
    "mirrorport",
    "module",
    "multiple",
    "native",
    "object",
    "octet",
    "oneway",
    "out",
    "primarykey",
    "private",
    "port",
    "porttype",
    "provides",
    "public",
    "publishes",
    "raises",
    "readonly",
    "setraises",
    "sequence",
    "short",
    "string",
    "struct",
    "supports",
    "switch",
    "true",
    "truncatable",
    "typedef",
    "typeid",
    "typename",
    "typeprefix",
    "unsigned",
    "union",
    "uses",
    "valuebase",
    "valuetype",
    "void",
    "wchar",
    "wstring",
    "int8",
    "uint8",
    "int16",
    "int32",
    "int64",
    "uint16",
    "uint32",
    "uint64",
];

/// ROS 2 (rosidl)が生成するC/C++コードで実際に使われるC/C++の予約語。
/// IDL_KEYWORDSと重複するもの(struct/const/true等)はそちらにのみ載せている。
const C_CPP_KEYWORDS: &[&str] = &[
    "auto",
    "break",
    "continue",
    "do",
    "else",
    "extern",
    "for",
    "goto",
    "if",
    "inline",
    "int",
    "register",
    "restrict",
    "return",
    "signed",
    "sizeof",
    "static",
    "volatile",
    "while",
    "alignas",
    "alignof",
    "and",
    "and_eq",
    "asm",
    "atomic_cancel",
    "atomic_commit",
    "atomic_noexcept",
    "audit",
    "axiom",
    "bitand",
    "bitor",
    "bool",
    "catch",
    "char8_t",
    "char16_t",
    "char32_t",
    "class",
    "co_await",
    "co_return",
    "co_yield",
    "compl",
    "concept",
    "const_cast",
    "consteval",
    "constexpr",
    "constinit",
    "decltype",
    "delete",
    "dynamic_cast",
    "explicit",
    "export",
    "friend",
    "mutable",
    "namespace",
    "new",
    "noexcept",
    "not",
    "not_eq",
    "nullptr",
    "operator",
    "or",
    "or_eq",
    "protected",
    "reflexpr",
    "reinterpret_cast",
    "requires",
    "template",
    "this",
    "thread_local",
    "throw",
    "try",
    "using",
    "virtual",
    "wchar_t",
    "xor",
    "xor_eq",
];

/// rosidl_generator_pyが生成するPythonバインディングの予約語(`keyword.kwlist`相当、小文字化済み)。
/// `match`/`case`/`type`/`_`のようなソフトキーワード(特定の文脈でのみ意味を持つ)は、
/// 属性名/識別子としては現在も合法なため意図的に含めない
/// (実際`type`はshape_msgs/SolidPrimitive等で実在するROS 2フィールド名でもある)。
/// IDL_KEYWORDS/C_CPP_KEYWORDSと重複するもの(false/true/and/or/not/import/in等)は
/// そちらにのみ載せている。
const PYTHON_KEYWORDS: &[&str] = &[
    "as", "assert", "async", "await", "def", "del", "elif", "except", "finally", "from", "global",
    "is", "lambda", "nonlocal", "none", "pass", "raise", "with", "yield",
];

/// ROS 2 .msg のフィールド名規則: `[a-z][a-z0-9_]*` かつ `__` を含まず `_` で終わらない
/// (https://design.ros2.org/articles/interface_definition.html)。
/// 加えて、生成されるIDL(struct/module本体)およびrosidlが吐くC/C++・Pythonコードの識別子と
/// 衝突する名前は、形式上は`[a-z][a-z0-9_]*`に合致していても不正な出力になるため拒否する。
/// 例: `r#struct`はRustの識別子としては合法だが、unraw後の`struct`はIDLキーワードであり
/// `struct Foo { ... string struct; ... };`のような構文エラーを生成してしまう。
fn validate_msg_field_name(name: &str) -> Result<(), String> {
    validate_lower_snake_case(name)?;
    reject_reserved_keyword(name, "a .msg/IDL field name")
}

/// IDL/C/C++/Pythonの予約語かどうかを判定する。フィールド名・パッケージ名は
/// どちらも小文字強制のため、キーワードリストとの単純な文字列一致判定を共有できる。
fn is_reserved_keyword(name: &str) -> bool {
    IDL_KEYWORDS.contains(&name)
        || C_CPP_KEYWORDS.contains(&name)
        || PYTHON_KEYWORDS.contains(&name)
}

/// 予約語であれば`context`を埋め込んだエラーにする。呼び出し側ごとに用途が違うため
/// メッセージへの埋め込みは呼び出し側に委ねる(一般化すると具体性を失うため)。
fn reject_reserved_keyword(name: &str, context: &str) -> Result<(), String> {
    if is_reserved_keyword(name) {
        return Err(format!(
            "`{name}` is a reserved IDL/C/C++/Python keyword and cannot be used as {context}"
        ));
    }
    Ok(())
}

/// IDL4のエスケープ識別子(先頭`_`)を使って予約語との衝突を避ける。ROS 2のIDL生成
/// (rosidl_adapter)と同じ方式。パッケージ名はIDLの`module`識別子としてそのまま
/// 埋め込まれるため、リネーム以外に回避手段がない予約語(実在するROS 2パッケージ名にも
/// `map`/`object`等が含まれる)との衝突はエラーではなくエスケープで解決する
/// (フィールド名側は`reject_reserved_keyword`で引き続き拒否し、挙動を変えない)。
fn escape_idl_identifier(name: &str) -> std::borrow::Cow<'_, str> {
    if is_reserved_keyword(name) {
        std::borrow::Cow::Owned(format!("_{name}"))
    } else {
        std::borrow::Cow::Borrowed(name)
    }
}

/// ROS 2 パッケージ名規則(REP 144): `[a-z][a-z0-9_]*` かつ `__` を含まず `_` で終わらない。
/// 予約語との衝突はここでは拒否しない。生成されるIDLの`module`名として使う際は
/// `escape_idl_identifier`でエスケープする(パッケージ名は`_`で終われずリネームでしか
/// 回避できない予約語もあるため、フィールド名と違い拒否ではなくエスケープを選ぶ)。
fn validate_ros_package_name(name: &str) -> Result<(), String> {
    validate_lower_snake_case(name)
}

/// ROS 2 メッセージ型名規則: UpperCamelCase `[A-Z][A-Za-z0-9]*`（underscore不可）。
fn validate_ros_type_name(name: &str) -> Result<(), String> {
    let mut chars = name.chars();
    let first = match chars.next() {
        Some(c) => c,
        None => return Err("must not be empty".to_string()),
    };
    if !first.is_ascii_uppercase() {
        return Err(format!(
            "must start with an uppercase ASCII letter, found `{first}`"
        ));
    }
    for c in chars {
        if !c.is_ascii_alphanumeric() {
            return Err(format!(
                "must contain only ASCII letters and digits (no `_` or `-`), found `{c}`"
            ));
        }
    }
    Ok(())
}

/// 旧`cdds(typename = "pkg/Name")`記法を検出したときのエラーを組み立てる。
/// Why: `package`/`name`への一本化後、`typename`キーは無視されるので、ユーザーに明示的にエラーで知らせる
fn typename_attr_removed_error(path: &syn::Path) -> syn::Error {
    syn::Error::new_spanned(
        path,
        "#[cdds(typename = \"...\")] is no longer supported; use #[cdds(package = \"...\", name = \"...\")] instead",
    )
}

/// `#[cdds(...)]`から読み取った生の値。`Topic`の`Container`と`DdsInterface`の
/// `DdsInterfaceContainer`はこれを共通の入力として、それぞれ必要な後処理(typename組み立て・
/// package/nameの命名規則検証)を行う。
struct CddsAttrs {
    fixed_size: bool,
    package: Option<syn::LitStr>,
    name: Option<syn::LitStr>,
}

/// 未知キーと、キー名ですらないトークンの両方に使う共通メッセージ。
/// Why: synが返す`expected identifier`ではどの属性の何が悪いのか読み取れない
const UNKNOWN_CDDS_KEY_MESSAGE: &str =
    "unknown cdds attribute key (expected `fixed_size`, `package`, or `name`)";

/// `#[cdds(...)]`の引数1つ分。
/// Why: `syn::Meta`をそのままパースすると`#[cdds("foo")]`のようにキー名で始まらない入力が
///      syn内部のパスパースエラーになる。キー名の位置の誤りは常にcdds独自のメッセージで返したい
struct CddsArg(syn::Meta);

impl syn::parse::Parse for CddsArg {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        if input.fork().parse::<syn::Path>().is_err() {
            return Err(syn::Error::new(input.span(), UNKNOWN_CDDS_KEY_MESSAGE));
        }
        input.parse().map(CddsArg)
    }
}

/// `cdds`属性を`(fixed_size, package, name)`へ読み取る共通実装。
/// 以前は`Container::parse`/`DdsInterfaceContainer::parse`がほぼ同型のコードを別々に持ち、
/// 両方とも以下を黙って無視する防御的すぎるパターンマッチだった:
/// - `#[cdds(...)]`の引数リストのパースエラー(属性の構文エラー)
/// - `fixed_size`/`package`/`name`/(旧)`typename`以外の未知キー(typo等)
/// - `package`/`name`への非文字列リテラル(`package = 42`等)
/// - 同キーの重複指定(複数`#[cdds]`属性行・同一行内のどちらでも、後勝ちで黙って上書きしていた)
///
/// ここでは全てcompile_errorとして伝播/拒否する。「複数`#[cdds]`属性行」自体は合法のまま
/// (禁止されるのはキー単位の重複)。
fn parse_cdds_attrs(attrs: &[syn::Attribute]) -> Result<CddsAttrs, syn::Error> {
    let mut fixed_size = false;
    let mut package: Option<syn::LitStr> = None;
    let mut name: Option<syn::LitStr> = None;

    for attr in attrs {
        if attr.path().get_ident().map(|i| i == "cdds") != Some(true) {
            continue;
        }
        let syn::Meta::List(meta_list) = &attr.meta else {
            return Err(syn::Error::new_spanned(
                attr,
                "#[cdds(...)] expects a parenthesized attribute list, e.g. #[cdds(package = \"...\")]",
            ));
        };
        let nested = meta_list.parse_args_with(
            syn::punctuated::Punctuated::<CddsArg, syn::Token![,]>::parse_terminated,
        )?;
        for CddsArg(meta) in nested {
            match meta {
                syn::Meta::Path(path) if path.is_ident("fixed_size") => {
                    if fixed_size {
                        return Err(syn::Error::new_spanned(
                            path,
                            "duplicate cdds attribute key `fixed_size`",
                        ));
                    }
                    fixed_size = true;
                }
                syn::Meta::NameValue(nv) if nv.path.is_ident("package") => {
                    if package.is_some() {
                        return Err(syn::Error::new_spanned(
                            &nv.path,
                            "duplicate cdds attribute key `package`",
                        ));
                    }
                    package = Some(expect_cdds_lit_str(&nv.value, "package")?);
                }
                syn::Meta::NameValue(nv) if nv.path.is_ident("name") => {
                    if name.is_some() {
                        return Err(syn::Error::new_spanned(
                            &nv.path,
                            "duplicate cdds attribute key `name`",
                        ));
                    }
                    name = Some(expect_cdds_lit_str(&nv.value, "name")?);
                }
                syn::Meta::NameValue(nv) if nv.path.is_ident("typename") => {
                    return Err(typename_attr_removed_error(&nv.path));
                }
                other => {
                    return Err(syn::Error::new_spanned(other, UNKNOWN_CDDS_KEY_MESSAGE));
                }
            }
        }
    }

    Ok(CddsAttrs {
        fixed_size,
        package,
        name,
    })
}

/// `package`/`name`の値が文字列リテラルであることを検証する
fn expect_cdds_lit_str(expr: &syn::Expr, key: &str) -> Result<syn::LitStr, syn::Error> {
    match expr {
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(lit_str),
            ..
        }) => Ok(lit_str.clone()),
        other => Err(syn::Error::new_spanned(
            other,
            format!("#[cdds({key} = ...)] expected a string literal"),
        )),
    }
}

/// Containerアトリビュートのパース結果を保持する構造体
struct Container {
    fixed_size: bool,
    typename: proc_macro2::TokenStream,
}

impl Container {
    /// `cdds(package = ..., name = ...)`は`Topic`と`DdsInterface`の両方が共有する唯一の
    /// 型名情報源。以前は`cdds(typename = ...)`と`dds_interface(package/name)`が別属性として
    /// 独立に設定できてしまい、両方をderiveすると値がずれ得た
    /// (ずれるとros2idl/mcap再現時にパース不能になる)。属性そのものを一本化することで、
    /// 生成コード側の分岐(featureで各deriveを個別に有効化する等)によらず矛盾が発生しなくなる。
    fn parse(item: &syn::ItemStruct) -> Result<Self, syn::Error> {
        let CddsAttrs {
            fixed_size,
            package,
            name,
        } = parse_cdds_attrs(&item.attrs)?;

        let typename = if let Some(package) = package {
            let name = name.unwrap_or_else(|| {
                syn::LitStr::new(&item.ident.unraw().to_string(), item.ident.span())
            });
            let full_name = format!("{}/{}", package.value(), name.value());
            quote! {
                fn typename() -> ::std::ffi::CString {
                    ::std::ffi::CString::new(#full_name).expect("Unable to create CString for type name")
                }
            }
        } else {
            quote! {}
        };

        Ok(Container {
            fixed_size,
            typename,
        })
    }
}

// create the keyhash methods for this type
// PID_KEY_HASH仕様に従い、フィールドの順番通りにCDRエンコーディングを行う構造体
fn create_keyhash_functions(item: &syn::ItemStruct) -> proc_macro2::TokenStream {
    let topic_key_ident = &item.ident;
    let topic_key_holder_ident = quote::format_ident!("{}KeyHolder_", &item.ident);
    let Container {
        fixed_size,
        typename,
    } = match Container::parse(item) {
        Ok(container) => container,
        Err(err) => return err.to_compile_error(),
    };

    let mut ts = quote! {
        impl TopicType for #topic_key_ident {
            /// return the cdr encoding for the key. The encoded string includes the four byte
            /// encapsulation string.
            fn key_cdr(&self) -> ::std::vec::Vec<u8> {
                let holder_struct : #topic_key_holder_ident = self.into();
                let encoded = cdr::serialize::<_, _, cdr::CdrBe>(&holder_struct, cdr::Infinite).expect("Unable to serialize key");
                encoded
            }

            fn is_fixed_size() -> bool {
                #fixed_size
            }

            fn has_key() -> bool {
                ::std::mem::size_of::<#topic_key_holder_ident>() > 0
            }

            fn force_md5_keyhash() -> bool {
                 #topic_key_holder_ident::is_variable_length()
            }

            #typename
        }
    };

    if fixed_size {
        let ts_fixed = quote! {
            impl FixedTopicType for #topic_key_ident {}
        };
        ts.extend(ts_fixed);
    }

    ts
}

// Util関数の追加。型からTopicを作成したり、サンプルバッファを作成したりする
fn create_topic_functions(item: &syn::ItemStruct) -> proc_macro2::TokenStream {
    let topic_key_ident = &item.ident;

    let ts = quote! {
        impl #topic_key_ident {
            /// Create a topic using of this Type specifying the topic name
            ///
            /// # Arguments
            ///
            /// * `participant` - The participant handle onto which this topic should be created
            /// * `name` - The name of the topic
            /// * `maybe_qos` - A QoS structure for this topic.  The Qos is optional
            /// * `maybe_listener` - A listener to use on this topic. The listener is optional
            ///
            pub fn create_topic_with_name(
                participant: &DdsParticipant,
                name: &str,
                maybe_qos: ::std::option::Option<DdsQos>,
                maybe_listener: ::std::option::Option<DdsListener>,
            ) -> ::std::result::Result<DdsTopic::<Self>, DDSError> {
                DdsTopic::<Self>::create(participant,name, maybe_qos,maybe_listener)
            }

            /// Create a topic of this Type using the default topic name. The default topic
            /// name is provided by the Self::topic_name function.
            /// # Arguments
            ///
            /// * `participant` - The participant handle onto which this topic should be created
            /// * `maybe_topic_prefix` - An additional prefix to be added to the topic name. This can be None
            /// * `maybe_qos` - A QoS structure for this topic.  The Qos is optional
            /// * `maybe_listener` - A listener to use on this topic. The listener is optional
            ///
            pub fn create_topic(
                participant: &DdsParticipant,
                maybe_topic_prefix: ::std::option::Option<&str>,
                maybe_qos: ::std::option::Option<DdsQos>,
                maybe_listener: ::std::option::Option<DdsListener>,
            ) -> ::std::result::Result<DdsTopic::<Self>, DDSError> {
                let name = #topic_key_ident::topic_name(maybe_topic_prefix);
                DdsTopic::<Self>::create(participant,&name, maybe_qos,maybe_listener)
            }

            /// Create a sample buffer for storing an array of samples
            /// You can pass the sample buffer into a read to read multiple
            /// samples. Multiple samples are useful when you have one or more
            /// keys in your topic structure. Each value of the key will result in
            /// the storage of another sample.
            pub fn create_sample_buffer(len: usize) -> SampleBuffer<#topic_key_ident> {
                SampleBuffer::new(len)
            }
        }
    };

    ts
}

/// InterfaceRef/InterfaceDefを実装し、DDS IDLとROS 2 .msg定義を導出できるようにするderiveマクロ
///
/// マクロはフィールド型の中身を解釈せず、すべて `InterfaceRef`/`InterfaceDef` トレイト呼び出しに
/// 委譲したコードを生成する。フィールド型がトレイトを実装していなければコンパイルエラーになる。
/// 生成コードは `cyclonedds_rs::interface_gen` の型がスコープにある前提（`use cyclonedds_rs::*;`）で
/// 既存の `Topic` derive と同じ規約に従う。
///
/// 生成される主なメソッド: `T::idl()`（依存を含む完全なIDL）、`T::ros2_msg()`（自身の.msgのみ）、
/// `T::full_ros2_msg()`（mcap/rosbag2向けの依存込み連結メッセージ定義）
///
/// # Attributes
/// `Topic`と同じ`cdds`属性を共有する（型名の情報源を一本化するため。詳細は`Topic`のdocを参照）。
/// * `cdds(package = "my_robot_interfaces")` - 所属するROS 2/DDSパッケージ名。省略不可
/// * `cdds(name = "String")` - .msg/IDL上の型名を上書きする。Rust側のstruct名を
///   予約名・prelude型との衝突を避けて改名しつつ、ワイヤ上の型名を維持したい場合に使う
///
/// # Field Attributes
/// * `topic_key` / `topic_key_enum` - `Topic`と共有するキー指定属性。`Topic`を併用しない
///   `DdsInterface`単独の構造体でも書けるようにヘルパー属性として登録している
///   （同名ヘルパー属性は複数deriveが登録しても合法。併用時はどちらのderiveも同じ
///   フィールド属性を読むだけなので情報源はずれない）。キー指定されたフィールドは
///   `idl_def()`が出力するIDLのフィールド宣言に`@key `が前置される
///   （`.msg`にはキーの記法が無いため`msg_def()`/`ros2_msg()`は変化しない）
///
/// # 対応する構造体
/// 名前付きフィールドを持つstructのみ対応。タプルstruct・unit struct・enum等はcompile_errorになる。
#[proc_macro_derive(DdsInterface, attributes(cdds, topic_key, topic_key_enum))]
pub fn derive_dds_interface(item: TokenStream) -> TokenStream {
    derive_dds_interface_impl(item.into()).into()
}

fn derive_dds_interface_impl(item: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    let input = match syn::parse2::<syn::ItemStruct>(item) {
        Ok(item) => item,
        Err(err) => return err.to_compile_error(),
    };

    if let Err(err) = check_no_generics("DdsInterface", &input) {
        return err.to_compile_error();
    }

    if let Err(err) =
        check_reserved_collision("DdsInterface", &input, DDS_INTERFACE_RESERVED_IDENTS)
    {
        return err.to_compile_error();
    }

    let container = match DdsInterfaceContainer::parse(&input) {
        Ok(container) => container,
        Err(err) => return err.to_compile_error(),
    };
    let package = container.package;
    // IDLの`module`識別子・型参照にのみエスケープ後の名前を使う。ROS 2 .msg形式の
    // パス(msg_ref)や`InterfaceDef::package()`が返す値はROS 2上の実際のパッケージ名
    // でなければならないため、そちらは元の`package`のまま使う
    let escaped_package =
        syn::LitStr::new(&escape_idl_identifier(&package.value()), package.span());

    let named_fields = match &input.fields {
        syn::Fields::Named(named) => &named.named,
        _ => {
            return syn::Error::new_spanned(
                &input.ident,
                "#[derive(DdsInterface)] is only supported for structs with named fields",
            )
            .to_compile_error();
        }
    };

    // `struct Empty {}`(0フィールドのnamed struct)はIDL/`.msg`のstructが
    // メンバー1つ以上を要求する文法に違反する。ダミーメンバーを自動挿入すると
    // ワイヤ上のCDR(0 byte)とIDL/msg定義(1 byte以上)が食い違うため、自動修復はせず
    // ユーザーにフィールド追加を促すcompile_errorにする
    if named_fields.is_empty() {
        return syn::Error::new_spanned(
            &input.ident,
            "#[derive(DdsInterface)]: empty structs cannot be represented in IDL; add a field\n\
             (ROS 2 uses `uint8 structure_needs_at_least_one_member` for empty messages)",
        )
        .to_compile_error();
    }

    let struct_ident = &input.ident;
    let name_was_explicit = container.name.is_some();
    let struct_name = container.name.unwrap_or_else(|| {
        syn::LitStr::new(&struct_ident.unraw().to_string(), struct_ident.span())
    });

    // struct_nameはcdds(name=...)の明示値・struct identのunraw名へのフォールバックの
    // どちらかだが、どちらの経路でもROS 2メッセージ型名規則(UpperCamelCase)を満たす必要がある。
    // Rustの識別子規則はUpperCamelCaseを強制しないため、フォールバック値は無検証のままだと
    // 例えば`struct robot_status`が`struct robot_status { ... };`という規則違反のワイヤ型名を
    // そのまま出力してしまう。
    if let Err(reason) = validate_ros_type_name(&struct_name.value()) {
        let msg = if name_was_explicit {
            format!(
                "#[cdds(name = \"{}\")] is not a valid ROS 2 message type name: {}\n\
                 (must be UpperCamelCase: [A-Z][A-Za-z0-9]*, no underscores)",
                struct_name.value(),
                reason
            )
        } else {
            format!(
                "#[derive(DdsInterface)]: struct name `{}` is not a valid ROS 2 message type name: {}\n\
                 (must be UpperCamelCase: [A-Z][A-Za-z0-9]*, no underscores; override the wire name \
                 with #[cdds(name = \"...\")] if you want to keep this Rust identifier)",
                struct_name.value(),
                reason
            )
        };
        return syn::Error::new_spanned(&struct_name, msg).to_compile_error();
    }

    let field_types: Vec<&syn::Type> = named_fields.iter().map(|f| &f.ty).collect();
    // unraw後のフィールド名がROS 2の.msgフィールド名規則を満たすか検証する。
    // `_leading`/`camelCase`/`double__underscore`/`trailing_`/非ASCII識別子はRustでは合法だが
    // .msg/IDLのフィールド名としては不正であり、無検証のまま埋め込むと不正な定義を出力していた。
    for f in named_fields {
        let ident = f.ident.as_ref().unwrap();
        let name = ident.unraw().to_string();
        if let Err(reason) = validate_msg_field_name(&name) {
            return syn::Error::new_spanned(
                ident,
                format!(
                    "#[derive(DdsInterface)]: field name `{name}` is not a valid ROS 2 field name: {reason}"
                ),
            )
            .to_compile_error();
        }
    }
    // (derive側・補助) フィールド型がリテラル`[T; 0]`のときは、使用箇所(monomorphize時点)
    // ではなく定義箇所でも早期にエラーにする。`const SIZE: usize = 0;`経由の非リテラルな0は
    // ここでは検出できないが、そちらは`src/interface_gen.rs`のinline const blockが拾う。
    for f in named_fields {
        if let syn::Type::Array(type_arr) = &f.ty
            && let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Int(lit_int),
                ..
            }) = &type_arr.len
            && lit_int
                .base10_parse::<u128>()
                .map(|v| v == 0)
                .unwrap_or(false)
        {
            return syn::Error::new_spanned(
                        &f.ty,
                        "#[derive(DdsInterface)]: zero-length arrays cannot be represented in ROS 2 .msg/IDL",
                    )
                    .to_compile_error();
        }
    }
    let field_name_lits: Vec<syn::LitStr> = named_fields
        .iter()
        .map(|f| {
            let ident = f.ident.as_ref().unwrap();
            // Why: r#type等の予約語フィールドは.msg/IDL上ではr#なしの素の名前が正しいため
            syn::LitStr::new(&ident.unraw().to_string(), ident.span())
        })
        .collect();
    // fix 6: `DdsInterface`単独でも`#[topic_key]`のフィールド型を検証する。
    // `Topic`併用時はkeyhash構築がキー型を拒否するが、単独だと`Vec<u32>`等の
    // sequenceがそのまま`@key sequence<...>`になり、DDSで不正なIDLを吐いていた。
    // Topic側のkeyhash構築と共通のvalidate_key_field_typeを使い、判定を一致させる。
    for field in named_fields {
        if is_key(field)
            && let Err(err) = validate_key_field_type(field)
        {
            return err.to_compile_error();
        }
    }
    // Why: `Topic`のkeyhash計算が使う`#[topic_key]`/`#[topic_key_enum]`を唯一の情報源として
    // IDL側の`@key`前置にも流用する（cdds属性のpackage/name共有と同じ設計判断）。
    // IDL定義での挙動に違いが出るため実装が必要
    let key_prefix_lits: Vec<syn::LitStr> = named_fields
        .iter()
        .map(|f| syn::LitStr::new(if is_key(f) { "@key " } else { "" }, Span::call_site()))
        .collect();

    // Why ::std完全修飾: proc-macro出力は呼び出し側の名前解決に従うため、呼び出し側で
    // `String`等がROS型(std_msgs/String由来のstruct等)にシャドーされていても
    // 生成コードが壊れないよう、preludeに頼らず絶対パスで参照する
    let ts = quote! {
        impl InterfaceRef for #struct_ident {
            fn idl_ref(mode: NsMode) -> ::std::string::String {
                mode.idl_type_ref(#escaped_package, #struct_name)
            }
            fn msg_ref() -> ::std::string::String {
                ::std::format!("{}/{}", #package, #struct_name)
            }
            fn collect_defs(reg: &mut DefRegistry) {
                // 自身を先にvisiting経路へ積んでから（循環検出のため）フィールドへ再帰し、
                // 戻ってから確定登録する（依存が先に来るトポロジカル順を保つため）。
                // 再帰・相互再帰的な型定義（ROS 2 .msg/IDLでは表現不能）はDefRegistry::enterが
                // panicで検出する
                if reg.enter::<Self>() {
                    #(<#field_types as InterfaceRef>::collect_defs(reg);)*
                    reg.finish::<Self>();
                }
            }
            fn collect_msg_defs(reg: &mut MsgDefRegistry) {
                // 自身を先に登録し、初出の依存先に限りそのフィールドへ再帰する
                // (mcap/rosbag2の連結メッセージ定義が期待する「自身→出現順の依存先」の順序を保つため)
                if reg.enter::<Self>() {
                    #(<#field_types as InterfaceRef>::collect_msg_defs(reg);)*
                    reg.finish::<Self>();
                }
            }
        }

        impl InterfaceElem for #struct_ident {}

        impl InterfaceDef for #struct_ident {
            fn package() -> &'static str {
                #package
            }
            fn name() -> &'static str {
                #struct_name
            }
            fn idl_def(mode: NsMode) -> ::std::string::String {
                let mut s = ::std::string::String::new();
                s.push_str(&mode.idl_module_open(#escaped_package));
                s.push_str(&::std::format!("struct {} {{\n", mode.idl_type_name(#struct_name)));
                #(
                    s.push_str(&::std::format!("  {}{}\n", #key_prefix_lits, <#field_types as InterfaceRef>::idl_field(#field_name_lits, mode)));
                )*
                s.push_str("};\n");
                s.push_str(&mode.idl_module_close());
                s
            }
            fn msg_def() -> ::std::string::String {
                let mut s = ::std::string::String::new();
                #(
                    s.push_str(&::std::format!("{} {}\n", <#field_types as InterfaceRef>::msg_ref(), #field_name_lits));
                )*
                s
            }
        }
    };

    ts
}

/// cddsアトリビュート(package/name)のパース結果を保持する構造体。
/// `Topic`側のContainerと同じ`cdds`属性を読む。属性を一本化することで、
/// `Topic`と`DdsInterface`を併用した際に型名の情報源がずれることを構造的に防ぐ
/// (詳細はContainerのdocコメント参照)。
struct DdsInterfaceContainer {
    package: syn::LitStr,
    /// .msg/IDL上の型名の上書き。未指定ならstruct identのunraw名を使う
    name: Option<syn::LitStr>,
}

impl DdsInterfaceContainer {
    fn parse(item: &syn::ItemStruct) -> Result<Self, syn::Error> {
        let CddsAttrs { package, name, .. } = parse_cdds_attrs(&item.attrs)?;

        let package = package.ok_or_else(|| {
            syn::Error::new_spanned(
                &item.ident,
                "#[derive(DdsInterface)] requires #[cdds(package = \"...\")]",
            )
        })?;

        // package/nameのリテラル値は展開時に確定しているため、ここで全ケースを
        // 検査できる。無検証のまま埋め込むと`module  {`(空package)や
        // `module My-Pkg/extra {`(IDL文法違反)のような不正なIDLを出力していた。
        // `Topic`側のContainer::parseは検査しない(typename()にしか使われず、
        // 既存資産の互換を壊さないため)。
        if let Err(reason) = validate_ros_package_name(&package.value()) {
            return Err(syn::Error::new_spanned(
                &package,
                format!(
                    "#[cdds(package = \"{}\")] is not a valid ROS 2 package name: {}\n\
                     (must match [a-z][a-z0-9_]*, no trailing or consecutive underscores; see REP 144)",
                    package.value(),
                    reason
                ),
            ));
        }
        // nameの検証はここでは行わない。未指定時のフォールバック値(struct identのunraw名)は
        // DdsInterfaceContainerがstruct identを持たないためここでは検証できず、
        // 明示/フォールバックの2箇所に分けると片方だけ直す回帰を招く。
        // derive_dds_interface_implでstruct_name確定後にまとめて検証する。

        Ok(DdsInterfaceContainer { package, name })
    }
}

/*
fn struct_has_key(it: &ItemStruct) -> bool {
    for field in &it.fields {
        if is_key(field) {
            return true
        }
    }
    false
}
*/

fn is_key(field: &Field) -> bool {
    for attr in &field.attrs {
        if let Some(ident) = attr.path().get_ident()
            && (ident == "topic_key" || ident == "topic_key_enum")
        {
            return true;
        }
    }
    false
}

// There is no way to find out if the field is an enum or a struct,
// so we need a special marker to indicate key enums
// which we will treat like primitives.
fn is_key_enum(field: &Field) -> bool {
    for attr in &field.attrs {
        if let Some(ident) = attr.path().get_ident()
            && ident == "topic_key_enum"
        {
            return true;
        }
    }
    false
}

/// pathが`Name`単独、または`std::module::Name`(先頭`::`と`alloc`起点も許容)を指すか判定する。
/// Why: 呼び出し側で`String`等がシャドーされる場合、フィールド型は`::std::string::String`と
/// 完全修飾で書くのが正であり、その表記もプリミティブとして分類できる必要がある。
/// 字句判定のため別物の同名型と区別はできない(マクロの原理的制約)。
fn is_std_path(path: &syn::Path, module: &str, name: &str) -> bool {
    let segments: Vec<String> = path.segments.iter().map(|s| s.ident.to_string()).collect();
    match segments.as_slice() {
        [single] => single == name,
        [krate, module_seg, name_seg] => {
            (krate == "std" || krate == "alloc") && module_seg == module && name_seg == name
        }
        _ => false,
    }
}

fn is_primitive_type_path(type_path: &syn::TypePath) -> bool {
    type_path.path.is_ident("bool")
        || type_path.path.is_ident("i8")
        || type_path.path.is_ident("i16")
        || type_path.path.is_ident("i32")
        || type_path.path.is_ident("i64")
        || type_path.path.is_ident("i128")
        || type_path.path.is_ident("isize")
        || type_path.path.is_ident("u8")
        || type_path.path.is_ident("u16")
        || type_path.path.is_ident("u32")
        || type_path.path.is_ident("u64")
        || type_path.path.is_ident("u128")
        || type_path.path.is_ident("usize")
        || type_path.path.is_ident("f32")
        || type_path.path.is_ident("f64")
        || is_std_path(&type_path.path, "string", "String")
}

// check if a field is of a primitive type. We assume anything not primitive
// is a struct
fn is_primitive(field: &Field) -> bool {
    if let syn::Type::Path(type_path) = &field.ty {
        is_primitive_type_path(type_path)
    } else {
        false
    }
}

// Is the length of the underlying type variable. This is needed
// According to the DDSI RTPS spec, the potential length of a field
// must be checked to decide whether to use md5 checksum for the key
// hash.  If a String (or Vec) is used as a key_field, then the
// length is variable.
fn is_variable_length(field: &Field) -> bool {
    if let syn::Type::Path(type_path) = &field.ty {
        is_std_path(&type_path.path, "vec", "Vec")
            || is_std_path(&type_path.path, "string", "String")
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 予約名`name`のstructをderiveした際に期待されるcompile_errorトークン列を組み立てる
    fn expected_rejection(derive_name: &str, name: &str) -> String {
        syn::Error::new(
            Span::call_site(),
            format!(
                "#[derive({})]: struct name `{}` collides with a cyclonedds-rs identifier used by the generated code; rename the Rust struct (for DdsInterface, keep the wire name with #[cdds(name = \"{}\")])",
                derive_name, name, name
            ),
        )
        .to_compile_error()
        .to_string()
    }

    // Why: 予約名denylistに一致するstructは生成コードが自壊するため、
    //      分かりやすいcompile_errorで事前拒否されることを保証する
    // Method: 各予約名のderive出力全体を期待するcompile_errorトークン列と完全比較する
    #[test]
    fn reserved_struct_names_are_rejected_with_compile_error() {
        for name in TOPIC_RESERVED_IDENTS {
            let ident = quote::format_ident!("{}", name);
            let actual = derive_topic_impl(quote! { struct #ident { x: u8 } }).to_string();
            assert_eq!(actual, expected_rejection("Topic", name));
        }
        for name in DDS_INTERFACE_RESERVED_IDENTS {
            let ident = quote::format_ident!("{}", name);
            let actual = derive_dds_interface_impl(quote! {
                #[cdds(package = "test_msgs")]
                struct #ident { x: u8 }
            })
            .to_string();
            assert_eq!(actual, expected_rejection("DdsInterface", name));
        }
    }

    // Why: denylistが通常のROS型名まで巻き込まないこと（過剰反応の否定）を保証する
    // Method: prelude衝突名を含む非予約名のリストでderive出力にcompile_errorが無いことを確認する
    #[test]
    fn non_reserved_struct_names_are_not_rejected() {
        for name in ["RobotStatus", "String", "Vec", "Time"] {
            let ident = quote::format_ident!("{}", name);
            let topic_out = derive_topic_impl(quote! { struct #ident { x: u8 } }).to_string();
            assert!(
                !topic_out.contains("compile_error"),
                "{}: {}",
                name,
                topic_out
            );
            let interface_out = derive_dds_interface_impl(quote! {
                #[cdds(package = "test_msgs")]
                struct #ident { x: u8 }
            })
            .to_string();
            assert!(
                !interface_out.contains("compile_error"),
                "{}: {}",
                name,
                interface_out
            );
        }
    }

    // Why: シャドーイング回避のため完全修飾で書かれたフィールド型もプリミティブと
    //      分類できる必要がある一方、無関係な同名パスまで受理しないことを保証する
    // Method: 受理/非受理の対応表をリスト化し、is_std_pathの実測値と比較する
    #[test]
    fn std_path_recognition_accepts_qualified_forms_only() {
        let cases = [
            ("String", true),
            ("std::string::String", true),
            ("::std::string::String", true),
            ("alloc::string::String", true),
            ("core::string::String", false),
            ("my::string::String", false),
            ("string::String", false),
            ("std::string::String2", false),
        ];
        let actual: Vec<(&str, bool)> = cases
            .iter()
            .map(|(path_str, _)| {
                let path: syn::Path = syn::parse_str(path_str).unwrap();
                (*path_str, is_std_path(&path, "string", "String"))
            })
            .collect();
        let expect: Vec<(&str, bool)> = cases.to_vec();
        assert_eq!(actual, expect);
    }

    // Why: Arc<u64>等のジェネリック型キーは従来「ArcKeyHolder_が見つからない」という
    //      生成コード内部事情のエラー(E0412)になっていたため、フィールド型を指す
    //      明確なcompile_errorで拒否されることを保証する
    // Method: ジェネリック型キーのリストでderive出力を期待するcompile_errorトークン列と完全比較
    #[test]
    fn generic_topic_key_types_are_rejected_with_clear_error() {
        let expect = syn::Error::new(
            Span::call_site(),
            "#[topic_key]: generic type is not supported as a key; keys must be primitives, [primitive; N] arrays, or structs deriving Topic",
        )
        .to_compile_error()
        .to_string();
        for ty_str in [
            "Arc<u64>",
            "Rc<u8>",
            "Mutex<u32>",
            "Vec<u8>",
            "Box<f32>",
            "Option<u32>",
            "std::sync::Arc<u64>",
        ] {
            let ty: syn::Type = syn::parse_str(ty_str).unwrap();
            let actual = derive_topic_impl(quote! {
                struct Bad {
                    #[topic_key]
                    k: #ty,
                }
            })
            .to_string();
            assert_eq!(actual, expect, "{}", ty_str);
        }
    }

    // Why: ジェネリック拒否が従来サポートしていたキー型
    //      (プリミティブ・完全修飾String・ネスト構造体・プリミティブ固定長配列)を
    //      巻き込まないことを保証する
    // Method: 各キー型のderive出力にcompile_errorが無いことをリストで確認する
    #[test]
    fn supported_topic_key_types_are_not_rejected() {
        for ty_str in [
            "u32",
            "String",
            "::std::string::String",
            "Point",
            "[u8; 16]",
        ] {
            let ty: syn::Type = syn::parse_str(ty_str).unwrap();
            let actual = derive_topic_impl(quote! {
                struct Good {
                    #[topic_key]
                    k: #ty,
                }
            })
            .to_string();
            assert!(!actual.contains("compile_error"), "{}: {}", ty_str, actual);
        }
    }

    // Why: `#[topic_key_enum]`は「列挙型をプリミティブ扱いする」マーカーのはずが、
    //      以前は型の形自体の検証(非ジェネリック単一パス)ごと素通りしていた。
    //      `Vec<u32>`のようなキーにできない型でもcompile_errorにならず、
    //      DdsInterface側は不正なIDL(sequenceを@keyにする等)を出力していた
    // Method: ジェネリック型・非プリミティブ要素の固定長配列のリストでderive出力に
    //         compile_errorが含まれることを確認する
    #[test]
    fn generic_topic_key_enum_types_are_rejected_with_clear_error() {
        let expect = syn::Error::new(
            Span::call_site(),
            "#[topic_key_enum]: enum keys must be a non-generic single-path type",
        )
        .to_compile_error()
        .to_string();
        for ty_str in ["Vec<u32>", "Option<u32>", "[Position; 4]"] {
            let ty: syn::Type = syn::parse_str(ty_str).unwrap();
            let actual = derive_topic_impl(quote! {
                struct Bad {
                    #[topic_key_enum]
                    k: #ty,
                }
            })
            .to_string();
            assert_eq!(actual, expect, "{}", ty_str);
        }
    }

    // Why: 列挙型プリミティブとして正当な用途(既存コードの利用形態)まで
    //      誤って拒否しないことを保証する(回帰検知)
    // Method: プリミティブ・非ジェネリック単一パス型それぞれのderive出力に
    //         compile_errorが無いことを確認する
    #[test]
    fn supported_topic_key_enum_types_are_not_rejected() {
        for ty_str in ["u8", "Position"] {
            let ty: syn::Type = syn::parse_str(ty_str).unwrap();
            let actual = derive_topic_impl(quote! {
                struct Good {
                    #[topic_key_enum]
                    k: #ty,
                }
            })
            .to_string();
            assert!(!actual.contains("compile_error"), "{}: {}", ty_str, actual);
        }
    }

    // Why: cdds(package/name)がTopic側のtypename()の情報源であることを保証する
    //      (dds_interface(package=...)相当のことをcddsだけで書けるようにするため)
    // Method: cdds(package=...)のみを付けたstructのderive出力に、
    //         "package/StructName"のtypenameが含まれることを確認する
    #[test]
    fn topic_typename_is_derived_from_cdds_package() {
        let actual = derive_topic_impl(quote! {
            #[cdds(package = "example_msgs")]
            struct ExampleMsg { x: u8 }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
        assert!(
            actual.contains("example_msgs/ExampleMsg"),
            "expected derived typename in: {}",
            actual
        );
    }

    // Why: cdds(name = ...)で型名を上書きしている場合、typename()もその上書き後の
    //      名前を使わなければ、DdsInterface側が生成する名前とずれてしまう
    // Method: nameを上書きしたstructのderive出力に上書き後の名前が使われていることを確認する
    #[test]
    fn topic_typename_uses_cdds_name_override() {
        let actual = derive_topic_impl(quote! {
            #[cdds(package = "example_msgs", name = "Renamed")]
            struct ExampleMsgImpl { x: u8 }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
        assert!(
            actual.contains("example_msgs/Renamed"),
            "expected overridden typename in: {}",
            actual
        );
        assert!(!actual.contains("ExampleMsgImpl\""));
    }

    // Why: 型名の情報源をcdds属性一つに統一した狙いは、TopicとDdsInterfaceを別々の
    //      cargo featureで出し分けて生成するコード(例: ros2msg自動生成)であっても、
    //      両方が有効になったときに矛盾した型名を持ちようがないことを保証すること。
    //      同じ`#[cdds(package/name)]`をTopic・DdsInterfaceそれぞれのderive実装に
    //      独立に食わせても、両者が同じpackage/nameから同じ型名を導出することを確認する
    // Method: derive_topic_implとderive_dds_interface_implの出力それぞれに、
    //         同じpackage/nameリテラルが埋め込まれていることを突き合わせる
    #[test]
    fn topic_and_dds_interface_derive_the_same_wire_name_from_one_cdds_attribute() {
        let input = quote! {
            #[cdds(package = "example_msgs", name = "Renamed")]
            struct ExampleMsg { x: u8 }
        };
        let topic_out = derive_topic_impl(input.clone()).to_string();
        let interface_out = derive_dds_interface_impl(input).to_string();
        assert!(!topic_out.contains("compile_error"), "{topic_out}");
        assert!(!interface_out.contains("compile_error"), "{interface_out}");
        // Topic::typename() embeds the pre-joined wire name.
        assert!(topic_out.contains("example_msgs/Renamed"), "{topic_out}");
        // DdsInterface joins package()/name() into the same wire name at runtime
        // (see msg_ref()'s format!("{}/{}", ...)), so it embeds the same literals.
        assert!(
            interface_out.contains("\"example_msgs\""),
            "{interface_out}"
        );
        assert!(interface_out.contains("\"Renamed\""), "{interface_out}");
    }

    // Why: DdsInterfaceを併用しないTopic単体のstructでも、cdds(package=...)による
    //      明示的な型名指定が引き続き使えることを保証する
    // Method: DdsInterfaceなしでcdds(package/name)のみ指定したstructの出力を確認する
    #[test]
    fn cdds_package_works_without_dds_interface() {
        let actual = derive_topic_impl(quote! {
            #[cdds(package = "custom_msg", name = "CustomTypeName")]
            struct Custom { x: u8 }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{actual}");
        assert!(
            actual.contains("custom_msg/CustomTypeName"),
            "expected explicit typename in: {actual}",
        );
    }

    // Why: cdds(package/name)への一本化後、旧記法cdds(typename = ...)は黙って無視される
    //      だけになり、利用者が新記法への移行に気づけなかった。明示的なコンパイルエラーで
    //      新記法への案内をすることを保証する
    // Method: cdds(typename = ...)を付けたstructのderive出力にcompile_errorと
    //         package/nameへの案内文が含まれることを確認する
    #[test]
    fn topic_cdds_typename_attr_is_rejected() {
        let actual = derive_topic_impl(quote! {
            #[cdds(typename = "pkg/Name")]
            struct Foo { x: u8 }
        })
        .to_string();
        assert!(actual.contains("compile_error"), "{actual}");
        assert!(actual.contains("cdds(package"), "{actual}");

        let actual = derive_dds_interface_impl(quote! {
            #[cdds(typename = "pkg/Name")]
            struct Foo { x: u8 }
        })
        .to_string();
        assert!(actual.contains("compile_error"), "{actual}");
        assert!(actual.contains("cdds(package"), "{actual}");
    }

    // Why: タプルstructのフィールドはidentを持たず、以前は
    //      `field.ident.unwrap()`がproc-macro panic("called `Option::unwrap()` on a `None`")に
    //      なっていた。名前付きフィールドを促す明確なcompile_errorで拒否されることを保証する
    // Method: `struct TupleKey(#[topic_key] u32);`のderive出力を期待するcompile_errorトークン列と完全比較
    #[test]
    fn tuple_struct_topic_key_is_rejected_with_compile_error() {
        let expect = syn::Error::new(
            Span::call_site(),
            "#[topic_key]: tuple struct fields cannot be keys; use a struct with named fields",
        )
        .to_compile_error()
        .to_string();
        let actual = derive_topic_impl(quote! {
            struct TupleKey(#[topic_key] u32);
        })
        .to_string();
        assert_eq!(actual, expect);
    }

    // Why: DDSでsequenceはキーにできないため、Topic側と共通の検査でcompile_error拒否されることを保証する
    // Method: ジェネリック型キーのリストでDdsInterface単独のderive出力を
    //         期待するcompile_errorトークン列と完全比較する
    #[test]
    fn dds_interface_generic_topic_key_types_are_rejected() {
        let expect = syn::Error::new(
            Span::call_site(),
            "#[topic_key]: generic type is not supported as a key; keys must be primitives, [primitive; N] arrays, or structs deriving Topic",
        )
        .to_compile_error()
        .to_string();
        for ty_str in ["Vec<u32>", "Option<u32>", "Arc<u64>", "Box<f32>"] {
            let ty: syn::Type = syn::parse_str(ty_str).unwrap();
            let actual = derive_dds_interface_impl(quote! {
                #[cdds(package = "test_msgs")]
                struct Bad {
                    #[topic_key]
                    k: #ty,
                }
            })
            .to_string();
            assert_eq!(actual, expect, "{}", ty_str);
        }
    }

    // Why: キー型検査がサポート済みキー型(プリミティブ・完全修飾String・
    //      ネスト構造体・プリミティブ固定長配列)を許可することを保証する
    // Method: 各キー型でDdsInterface単独のderive出力にcompile_errorが無いことをリストで確認する
    #[test]
    fn dds_interface_supported_topic_key_types_are_not_rejected() {
        for ty_str in [
            "u32",
            "String",
            "::std::string::String",
            "Point",
            "[u8; 16]",
        ] {
            let ty: syn::Type = syn::parse_str(ty_str).unwrap();
            let actual = derive_dds_interface_impl(quote! {
                #[cdds(package = "test_msgs")]
                struct Good {
                    #[topic_key]
                    k: #ty,
                }
            })
            .to_string();
            assert!(!actual.contains("compile_error"), "{}: {}", ty_str, actual);
        }
    }

    // Why: 生成impl(`impl Trait for #struct_ident`等)がジェネリクスを引き継がないため、
    //      ジェネリクス/ライフタイム付きstructをderiveすると生成コード内部事情のE0425/E0107/E0726等の
    //      難解なエラーになっていた。入り口で明確なcompile_errorとして拒否されることを保証する
    // Method: `<T>`/`<'a>`/`<const N: usize>`の3形をTopic・DdsInterface両方でderiveし、
    //         期待するcompile_errorトークン列と完全比較する
    #[test]
    fn generic_or_lifetime_structs_are_rejected_with_compile_error() {
        let cases: &[proc_macro2::TokenStream] = &[
            quote! { struct Wrapper<T> { value: T } },
            quote! { struct Borrowed<'a> { value: &'a u32 } },
            quote! { struct Sized<const N: usize> { value: [u8; N] } },
        ];

        for case in cases {
            let expect_topic = syn::Error::new(
                Span::call_site(),
                "#[derive(Topic)]: generic or lifetime parameters are not supported",
            )
            .to_compile_error()
            .to_string();
            let actual_topic = derive_topic_impl(case.clone()).to_string();
            assert_eq!(actual_topic, expect_topic, "{case}");

            let expect_interface = syn::Error::new(
                Span::call_site(),
                "#[derive(DdsInterface)]: generic or lifetime parameters are not supported",
            )
            .to_compile_error()
            .to_string();
            let with_cdds = quote! {
                #[cdds(package = "test_msgs")]
                #case
            };
            let actual_interface = derive_dds_interface_impl(with_cdds).to_string();
            assert_eq!(actual_interface, expect_interface, "{}", case);
        }
    }

    // Why: `struct Empty {}`は`syn::Fields::Named`(0フィールド)としてunit structの
    //      「named fieldsのみ対応」チェックをすり抜け、structメンバー1つ以上を要求する
    //      IDL/`.msg`の文法に違反する定義を出力していた。ダミーメンバーの自動挿入は
    //      ワイヤ(0 byte CDR)とIDL/msg定義の不一致を招くため、compile_errorで拒否されることを保証する
    // Method: 0フィールドstructの derive出力を期待するcompile_errorトークン列と完全比較する
    #[test]
    fn empty_named_struct_is_rejected_with_compile_error() {
        let expect = syn::Error::new(
            Span::call_site(),
            "#[derive(DdsInterface)]: empty structs cannot be represented in IDL; add a field\n\
             (ROS 2 uses `uint8 structure_needs_at_least_one_member` for empty messages)",
        )
        .to_compile_error()
        .to_string();
        let actual = derive_dds_interface_impl(quote! {
            #[cdds(package = "test_msgs")]
            struct Empty {}
        })
        .to_string();
        assert_eq!(actual, expect);
    }

    // Why: 0フィールド拒否が1フィールド以上のstructまで巻き込まないことを保証する
    #[test]
    fn single_field_named_struct_is_not_rejected() {
        let actual = derive_dds_interface_impl(quote! {
            #[cdds(package = "test_msgs")]
            struct NotEmpty { x: u8 }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
    }

    // Why: `_leading`/`camelCase`/`double__underscore`/`trailing_`/非ASCII識別子は
    //      Rustでは合法なフィールド名だが、ROS 2の.msgフィールド名規則
    //      ([a-z][a-z0-9_]*, 連続/末尾underscore禁止)には違反する。無検証のまま.msg/IDLに
    //      埋め込むと不正な定義になるため、compile_errorで拒否されることを保証する
    // Method: 違反名リストそれぞれのderive出力にcompile_errorとフィールド名が含まれることを確認する
    #[test]
    fn invalid_field_names_are_rejected_with_compile_error() {
        for name in [
            "_leading",
            "camelCase",
            "double__underscore",
            "trailing_",
            "名前",
        ] {
            let ident = quote::format_ident!("{}", name);
            let actual = derive_dds_interface_impl(quote! {
                #[cdds(package = "test_msgs")]
                struct Bad { #ident: u8 }
            })
            .to_string();
            assert!(actual.contains("compile_error"), "{}: {}", name, actual);
            assert!(
                actual.contains(&format!(
                    "field name `{}` is not a valid ROS 2 field name",
                    name
                )),
                "{}: {}",
                name,
                actual
            );
        }
    }

    // Why: `[a-z][a-z0-9_]*`の形式は満たしていても、IDLキーワード(Table 7-6)や
    //      rosidlが生成するC/C++・Pythonコードの予約語と衝突する名前は不正なIDL/コードを
    //      生成してしまう。`r#struct`/`r#module`のようなraw識別子はRustでは合法だが、
    //      unraw後の素の名前がキーワードと衝突するケースを拒否できることを保証する
    // Method: IDL専用キーワード(module)・C/C++専用キーワード(class)・Python専用キーワード(lambda)・
    //      複数リストに載るキーワード(struct)それぞれのderive出力にcompile_errorが含まれることを確認する
    #[test]
    fn reserved_keyword_field_names_are_rejected_with_compile_error() {
        for name in ["module", "class", "lambda", "struct"] {
            let ident = quote::format_ident!("r#{}", name);
            let actual = derive_dds_interface_impl(quote! {
                #[cdds(package = "test_msgs")]
                struct Bad { #ident: u8 }
            })
            .to_string();
            assert!(actual.contains("compile_error"), "{}: {}", name, actual);
            assert!(
                actual.contains(&format!(
                    "field name `{}` is not a valid ROS 2 field name",
                    name
                )),
                "{}: {}",
                name,
                actual
            );
        }
    }

    // Why: 正常なフィールド名まで誤って拒否しないこと、および`r#type`のような
    //      予約語フィールドがunraw後は合法な名前として通ること
    //      (既存テスト`raw_identifier_fields_are_unrawed_in_msg_and_idl`との互換維持)を保証する
    // Method: 正常名リストのderive出力にcompile_errorが無いことを確認する
    #[test]
    fn valid_field_names_are_not_rejected() {
        let actual = derive_dds_interface_impl(quote! {
            #[cdds(package = "test_msgs")]
            struct Good { x: u8, status_code: u8, r#type: u8 }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
    }

    // Why: `cdds(package = "...")`の値は無検証で`module {package} {`に埋め込まれており、
    //      空文字列や`My-Pkg/extra`のようなIDL文法違反の値をそのまま出力していた。
    //      derive展開時にリテラルの中身を検証してcompile_errorで拒否されることを保証する
    // Method: 違反値リストそれぞれのderive出力にcompile_errorが含まれることを確認する
    #[test]
    fn invalid_cdds_package_values_are_rejected_with_compile_error() {
        for pkg in ["", "My-Pkg", "a/b", "1pkg", "a__b"] {
            let actual = derive_dds_interface_impl(quote! {
                #[cdds(package = #pkg)]
                struct Foo { x: u8 }
            })
            .to_string();
            assert!(actual.contains("compile_error"), "{}: {}", pkg, actual);
            assert!(
                actual.contains("is not a valid ROS 2 package name"),
                "{}: {}",
                pkg,
                actual
            );
        }
    }

    // Why: パッケージ名は`_`で終われず(REP 144)、`module`のような予約語との衝突を
    //      compile_errorで拒否するとリネーム以外の回避手段が無くなる
    //      (実在するROS 2パッケージ名にも`map`/`object`等の予約語衝突がある)。
    //      拒否ではなくIDL4のエスケープ識別子(先頭`_`)で解決することを保証する
    // Method: 予約語のパッケージ名でcompile_errorが出ないこと、IDL出力に使う
    //         型参照/module宣言の呼び出しにのみ`_`前置のエスケープ名が渡ることを確認する
    #[test]
    fn reserved_keyword_cdds_package_values_are_escaped_in_idl() {
        for pkg in ["module", "struct", "class", "long", "string"] {
            let actual = derive_dds_interface_impl(quote! {
                #[cdds(package = #pkg)]
                struct Foo { x: u8 }
            })
            .to_string();
            assert!(!actual.contains("compile_error"), "{}: {}", pkg, actual);
            let escaped = format!("_{pkg}");
            assert!(
                actual.contains(&format!("idl_type_ref (\"{escaped}\"")),
                "{}: {}",
                pkg,
                actual
            );
            assert!(
                actual.contains(&format!("idl_module_open (\"{escaped}\"")),
                "{}: {}",
                pkg,
                actual
            );
            // ROS 2 .msgパス(msg_ref)・package()はIDL識別子ではないため、
            // 実際のパッケージ名のまま変えない
            assert!(
                actual.contains(&format!("\"{{}}/{{}}\" , \"{pkg}\"")),
                "{}: {}",
                pkg,
                actual
            );
        }
    }

    // Why: `cdds(name = "...")`も同様に無検証だったため、`1BadName`のような
    //      IDL識別子違反の値がそのまま`struct 1BadName`として出力されていた。
    // Method: 違反値リストそれぞれのderive出力にcompile_errorが含まれることを確認する
    #[test]
    fn invalid_cdds_name_values_are_rejected_with_compile_error() {
        for name in ["1BadName", "bad_name", "bad-name", ""] {
            let actual = derive_dds_interface_impl(quote! {
                #[cdds(package = "test_msgs", name = #name)]
                struct Foo { x: u8 }
            })
            .to_string();
            assert!(actual.contains("compile_error"), "{}: {}", name, actual);
            assert!(
                actual.contains("is not a valid ROS 2 message type name"),
                "{}: {}",
                name,
                actual
            );
        }
    }

    // Why: 正常なpackage/name値まで誤って拒否しないことを保証する
    #[test]
    fn valid_cdds_package_and_name_values_are_not_rejected() {
        let actual = derive_dds_interface_impl(quote! {
            #[cdds(package = "geometry_msgs", name = "Point2D")]
            struct Foo { x: u8 }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
    }

    // Why: `#[cdds(name = "...")]`を省略した場合のフォールバック値(struct identのunraw名)は
    //      Rustの識別子規則がUpperCamelCaseを強制しないため無検証だと、`robot_status`のような
    //      snake_case構造体がそのままROS 2型名規則違反のワイヤ型名として出力されていた。
    //      明示時と同じ検証がフォールバック値にも適用され、compile_errorになることを保証する
    // Method: 非UpperCamelCaseな struct 識別子のリストでderive出力にcompile_errorと
    //         型名違反メッセージが含まれることを確認する
    #[test]
    fn fallback_struct_name_is_validated_as_ros_type_name() {
        for name in ["robot_status", "robotStatus", "Robot_Status"] {
            let ident = quote::format_ident!("{}", name);
            let actual = derive_dds_interface_impl(quote! {
                #[cdds(package = "test_msgs")]
                struct #ident { x: u8 }
            })
            .to_string();
            assert!(actual.contains("compile_error"), "{}: {}", name, actual);
            assert!(
                actual.contains("is not a valid ROS 2 message type name"),
                "{}: {}",
                name,
                actual
            );
        }
    }

    // Why: フォールバック値検証の追加が、UpperCamelCaseな正常struct名まで
    //      誤って拒否しないことを保証する(回帰検知)
    #[test]
    fn fallback_struct_name_with_upper_camel_case_is_not_rejected() {
        let actual = derive_dds_interface_impl(quote! {
            #[cdds(package = "test_msgs")]
            struct RobotStatus { x: u8 }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
    }

    // Why: `Topic`側の`Container::parse`はtypename()にしか使わないため、既存資産の
    //      互換を壊さないよう検証しない設計判断であることを保証する(回帰検知)
    #[test]
    fn topic_cdds_package_is_not_validated() {
        let actual = derive_topic_impl(quote! {
            #[cdds(package = "My-Pkg", name = "1BadName")]
            struct Foo { x: u8 }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
    }

    // Why: usize/isize/u128/i128はis_primitive_type_pathでプリミティブ扱いされるため
    //      従来Topicキーとして素通りしていたが、usize/isizeは幅がプラットフォーム依存でワイヤ
    //      互換性がなく、u128/i128はcdrクレートがシリアライズできず実行時エラーになる。
    //      明確なcompile_errorで拒否されることを保証する(単独パス・[T; N]配列要素の両方)
    // Method: 4型(および配列形)のキーでderive出力を期待するcompile_errorトークン列と完全比較する
    #[test]
    fn unportable_int_topic_key_types_are_rejected_with_compile_error() {
        let expect = syn::Error::new(
            Span::call_site(),
            "#[topic_key]: `usize`/`isize`/`u128`/`i128` have no portable CDR representation; use a fixed-width type (u8..u64, i8..i64)",
        )
        .to_compile_error()
        .to_string();
        for ty_str in ["usize", "isize", "u128", "i128", "[usize; 4]"] {
            let ty: syn::Type = syn::parse_str(ty_str).unwrap();
            let actual = derive_topic_impl(quote! {
                struct Bad {
                    #[topic_key]
                    k: #ty,
                }
            })
            .to_string();
            assert_eq!(actual, expect, "{}", ty_str);
        }
    }

    // Why: u64のような幅固定の整数キーまで巻き込まないことを保証する
    #[test]
    fn fixed_width_int_topic_key_is_not_rejected() {
        let actual = derive_topic_impl(quote! {
            struct Good {
                #[topic_key]
                k: u64,
            }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
    }

    // Why: `#[cdds(...)]`の不正形は黙って無視されず、原因を示すcompile_errorになることを保証する
    // Method: 不正形ごとに(入力属性, 期待メッセージ片)を並べ、生成トークンに含まれるか検証する
    #[test]
    fn cdds_attr_invalid_forms_are_rejected() {
        let cases = [
            (
                quote! {
                    #[cdds(package = 42)]
                    struct Foo { x: u8 }
                },
                "expected a string literal",
            ),
            (
                quote! {
                    #[cdds(fixed_sized)]
                    struct Foo { x: u8 }
                },
                "unknown cdds attribute key",
            ),
            (
                quote! {
                    #[cdds("foo")]
                    struct Foo { x: u8 }
                },
                "unknown cdds attribute key",
            ),
            (
                quote! {
                    #[cdds(package = )]
                    struct Foo { x: u8 }
                },
                "compile_error",
            ),
            (
                quote! {
                    #[cdds(package = "a", package = "b")]
                    struct Foo { x: u8 }
                },
                "duplicate cdds attribute key `package`",
            ),
        ];
        for (input, expect) in cases {
            let actual = derive_topic_impl(input).to_string();
            assert!(actual.contains("compile_error"), "{}", actual);
            assert!(actual.contains(expect), "{}", actual);
        }
    }

    // Why: 同キーの重複は、別々の`#[cdds]`属性行にまたがっていても
    //      同様にcompile_errorになるべきことを保証する(last-winsをやめる)
    #[test]
    fn cdds_attr_duplicate_key_across_multiple_attributes_is_rejected() {
        let actual = derive_topic_impl(quote! {
            #[cdds(package = "a")]
            #[cdds(package = "b")]
            struct Foo { x: u8 }
        })
        .to_string();
        assert!(actual.contains("compile_error"), "{}", actual);
        assert!(
            actual.contains("duplicate cdds attribute key `package`"),
            "{}",
            actual
        );
    }

    // Why: 「複数`#[cdds]`属性行」自体は合法のまま残す(禁止されるのはキー単位の重複)
    //      ことを保証する。別キーを別行に分けて書く形は引き続きエラーなしで通る
    #[test]
    fn cdds_multiple_attribute_lines_with_distinct_keys_are_still_legal() {
        let actual = derive_topic_impl(quote! {
            #[cdds(package = "pkg")]
            #[cdds(name = "Renamed")]
            struct Foo { x: u8 }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
        assert!(actual.contains("pkg/Renamed"), "{}", actual);
    }

    // Why: `fixed_size`単独のような既存の正常形が引き続き通ることを保証する
    #[test]
    fn cdds_fixed_size_alone_is_still_legal() {
        let actual = derive_topic_impl(quote! {
            #[cdds(fixed_size)]
            struct Foo { x: u8 }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
    }

    // Why: DdsInterface側でも同じ共通パーサ(parse_cdds_attrs)を使っており、
    //      未知キー・非文字列値の拒否が同様に効くことを保証する
    #[test]
    fn dds_interface_cdds_attr_unknown_key_is_rejected() {
        let actual = derive_dds_interface_impl(quote! {
            #[cdds(package = "test_msgs", fixed_sized)]
            struct Foo { x: u8 }
        })
        .to_string();
        assert!(actual.contains("compile_error"), "{}", actual);
        assert!(actual.contains("unknown cdds attribute key"), "{}", actual);
    }

    // Why(derive側・補助): リテラル`[T; 0]`はROS 2 .msg/IDLのどちらでも表現できない
    //      (使用箇所のpost-monomorphizationエラーとは別に、定義箇所で早期に分かる
    //      明確なcompile_errorを出す)。フィールド型がリテラル`[u8; 0]`のとき拒否されることを保証する
    #[test]
    fn zero_length_array_field_is_rejected_with_compile_error() {
        let expect = syn::Error::new(
            Span::call_site(),
            "#[derive(DdsInterface)]: zero-length arrays cannot be represented in ROS 2 .msg/IDL",
        )
        .to_compile_error()
        .to_string();
        let actual = derive_dds_interface_impl(quote! {
            #[cdds(package = "test_msgs")]
            struct Bad { a: [u8; 0] }
        })
        .to_string();
        assert_eq!(actual, expect);
    }

    // Why: 0拒否が正の長さの配列まで巻き込まないことを保証する
    #[test]
    fn positive_length_array_field_is_not_rejected() {
        let actual = derive_dds_interface_impl(quote! {
            #[cdds(package = "test_msgs")]
            struct Good { a: [u8; 4] }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
    }

    // Why: `[String; N]`キーは要素が可変長(String)にもかかわらず、以前は
    //      配列ブランチがvariable_lengthを更新せず常にfalseになっていた
    //      (単独`String`キーは正しくtrueになる非対称)。keyhashが16 byteを超え得る場合に
    //      RTPS仕様上必須のMD5 keyhash(force_md5_keyhash())が使われず、他DDS実装との
    //      keyhash不整合を招くバグだった。生成される`is_variable_length()`が
    //      `if ! true`(true相当)になることを保証する
    // Method: 既存テストのトークン列比較様式に合わせ、生成コードの文字列に
    //         `if ! true`が含まれることを確認する
    #[test]
    fn string_array_topic_key_is_variable_length() {
        let actual = derive_topic_impl(quote! {
            struct Good {
                #[topic_key]
                names: [String; 2],
            }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
        assert!(actual.contains("if ! true"), "{}", actual);
    }

    // Why: プリミティブ固定長配列([u8; 16]等)は可変長ではないため、
    //      引き続き`is_variable_length()`がfalse相当のままであることを保証する(回帰検知)
    #[test]
    fn primitive_array_topic_key_is_not_variable_length() {
        let actual = derive_topic_impl(quote! {
            struct Good {
                #[topic_key]
                ids: [u8; 16],
            }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
        assert!(actual.contains("if ! false"), "{}", actual);
    }

    // Why: KeyHolder_structは以前常に非pub(可視性なし)で生成されていたため、
    //      別モジュールのpub structをキーにすると生成コードが参照する`XKeyHolder_`が
    //      E0603(privateなstructの外部参照)になっていた。元structと同じ可視性を
    //      引き継ぐことを保証する
    // Method: `pub struct`のderive出力に`pub struct XKeyHolder_`が含まれることを確認する
    #[test]
    fn pub_struct_key_holder_inherits_visibility() {
        let actual = derive_topic_impl(quote! {
            pub struct Point {
                #[topic_key]
                pub id: u32,
            }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
        assert!(actual.contains("pub struct PointKeyHolder_"), "{}", actual);
    }

    // Why: 可視性なしの既存struct(非pub)は引き続き非pubのKeyHolder_を生成することを保証する
    //      (回帰検知: `#vis`が誤って常にpubを付与しないこと)
    #[test]
    fn private_struct_key_holder_stays_private() {
        let actual = derive_topic_impl(quote! {
            struct Point {
                #[topic_key]
                id: u32,
            }
        })
        .to_string();
        assert!(!actual.contains("compile_error"), "{}", actual);
        assert!(!actual.contains("pub struct PointKeyHolder_"), "{}", actual);
        assert!(actual.contains("struct PointKeyHolder_"), "{}", actual);
    }
}
