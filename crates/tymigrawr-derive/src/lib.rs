//! Provides derive macros for `tymigrawr::HasCrudFields`.
use quote::quote;
use syn::{
    Attribute, Data, DataStruct, DeriveInput, Fields, FieldsNamed, Ident, Type, WhereClause,
    WherePredicate,
};

/// Returns `true` if the type looks like `Option<...>`.
fn is_option_type(ty: &Type) -> bool {
    if let Type::Path(ref tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            return seg.ident == "Option";
        }
    }
    false
}

/// Check if the field has `#[json_text]` attribute and generate a compile error.
fn check_no_json_text_attr(atts: &[Attribute]) -> proc_macro2::TokenStream {
    if atts.iter().any(|att| {
        att.path
            .get_ident()
            .map(|id| id == "json_text")
            .unwrap_or(false)
    }) {
        quote! {
            compile_error!("#[json_text] is no longer supported. Use JsonText<T> type wrapper instead.")
        }
    } else {
        quote! {}
    }
}

/// Returns `true` if the type looks like `PrimaryKey<...>` or `AutoPrimaryKey<...>`.
fn is_primary_key_type(ty: &Type) -> bool {
    if let Type::Path(ref tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            let ident_str = format!("{}", seg.ident);
            return ident_str == "PrimaryKey" || ident_str == "AutoPrimaryKey";
        }
    }
    false
}

fn get_fields(ast: &Data) -> (Vec<Ident>, Vec<Type>, Vec<Vec<Attribute>>) {
    let fields = match *ast {
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named: ref x, .. }),
            ..
        }) => x,
        _ => panic!("Only named fields supported"),
    };

    let tys = fields.iter().map(|x| x.ty.clone()).collect();
    let identifiers = fields.iter().map(|x| x.ident.clone().unwrap()).collect();
    let atts = fields.iter().map(|x| x.attrs.clone()).collect();

    (identifiers, tys, atts)
}

fn gen_crud_fields(
    idents: &[Ident],
    tys: &[Type],
    atts: &[Vec<Attribute>],
) -> Vec<proc_macro2::TokenStream> {
    idents
        .iter()
        .zip(tys.iter().zip(atts))
        .map(|(ident, (ty, atts))| {
            let error = check_no_json_text_attr(atts);
            quote! {
                #error
                let mut #ident = <#ty as tymigrawr::IsCrudField>::field();
                #ident.name = stringify!(#ident);
                #ident
            }
        })
        .collect()
}

fn get_primary_key(
    idents: &[Ident],
    tys: &[Type],
) -> (
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
    proc_macro2::TokenStream,
) {
    // Find the field with PrimaryKey<T> or AutoPrimaryKey<T> type
    let mut primary_keys = idents.iter().zip(tys.iter()).filter_map(|(ident, ty)| {
        if is_primary_key_type(ty) {
            Some((ident.clone(), ty.clone()))
        } else {
            None
        }
    });

    match (primary_keys.next(), primary_keys.next()) {
        (Some((ident, ty)), None) => {
            // Exactly one primary key field found
            let set_impl = quote! {
                if let Some(pk) = <#ty as tymigrawr::IsCrudField>::maybe_from_value(&value) {
                    self.#ident = pk;
                }
            };
            (
                quote! {stringify!(#ident)},
                quote! {tymigrawr::IsCrudField::value(&self.#ident)},
                set_impl,
            )
        }
        (Some(_), Some(_)) => {
            // Multiple primary key fields
            (
                quote! {
                    compile_error!("struct must have exactly one primary key field (PrimaryKey<T> or AutoPrimaryKey<T>)")
                },
                quote! {},
                quote! {},
            )
        }
        (None, _) => {
            // No primary key field found
            (
                quote! {
                    compile_error!("struct must have exactly one primary key field (PrimaryKey<T> or AutoPrimaryKey<T>)")
                },
                quote! {},
                quote! {},
            )
        }
    }
}

fn gen_from_crud_fields(
    idents: &[Ident],
    tys: &[Type],
    atts: &[Vec<Attribute>],
) -> Vec<proc_macro2::TokenStream> {
    idents
        .iter()
        .zip(tys.iter().zip(atts))
        .map(|(ident, (ty, field_atts))| {
            let error = check_no_json_text_attr(field_atts);
            let is_option = is_option_type(ty);

            if is_option {
                quote! {
                    #error
                    let #ident = match fields.get(stringify!(#ident)) {
                        Some(v) => <#ty as tymigrawr::IsCrudField>::maybe_from_value(v),
                        None => None,
                    };
                }
            } else {
                let ident_value = quote::format_ident!("__value_{}", ident);
                quote! {
                    #error
                    let #ident_value = fields
                        .get(stringify!(#ident))
                        .ok_or_else(|| tymigrawr::HasCrudFieldsError {
                            value: tymigrawr::Value::None,
                            reason: format!("missing required field {}", stringify!(#ident)),
                        })?;
                    let #ident = match <#ty as tymigrawr::IsCrudField>::maybe_from_value(#ident_value) {
                        Some(v) => v,
                        None => {
                            return Err(tymigrawr::HasCrudFieldsError {
                                value: #ident_value.clone(),
                                reason: format!("failed to deserialize field {}", stringify!(#ident)),
                            })
                        }
                    };
                }
            }
        })
        .collect()
}

/// Generates per-field serialization expressions for `as_crud_fields()`.
fn gen_as_crud_field_pairs(
    idents: &[Ident],
    tys: &[Type],
    atts: &[Vec<Attribute>],
) -> Vec<proc_macro2::TokenStream> {
    idents
        .iter()
        .zip(tys.iter().zip(atts))
        .map(|(ident, (_ty, field_atts))| {
            let error = check_no_json_text_attr(field_atts);
            quote! {
                #error
                (stringify!(#ident), tymigrawr::IsCrudField::value(&self.#ident))
            }
        })
        .collect()
}

/// Macro for deriving structs that have normal CRUD-worthy fields.
#[proc_macro_derive(HasCrudFields)]
pub fn derive_crud_fields(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: DeriveInput = syn::parse_macro_input!(input);
    let name = input.ident;
    let (field_idents, field_tys, field_atts) = get_fields(&input.data);
    let mut generics = input.generics;
    {
        /// Adds trait constraints on each of the field types.
        ///
        /// All fields must implement `IsCrudField`.
        fn constrain_field_types(clause: &mut WhereClause, tys: &[Type], _atts: &[Vec<Attribute>]) {
            for ty in tys.iter() {
                let where_predicate: WherePredicate =
                    syn::parse_quote!(#ty : tymigrawr::IsCrudField);
                clause.predicates.push(where_predicate);
            }
        }

        let where_clause = generics.make_where_clause();
        constrain_field_types(where_clause, &field_tys, &field_atts)
    }

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let table_name = name.to_string().to_ascii_lowercase();
    let crud_fields = gen_crud_fields(&field_idents, &field_tys, &field_atts);
    let from_crud_fields = gen_from_crud_fields(&field_idents, &field_tys, &field_atts);
    let as_crud_field_pairs = gen_as_crud_field_pairs(&field_idents, &field_tys, &field_atts);
    let (primary_key, primary_key_val, set_primary_key) =
        get_primary_key(&field_idents, &field_tys);
    let output = quote! {
        #[automatically_derived]
        impl #impl_generics tymigrawr::HasCrudFields for #name #ty_generics #where_clause {
            fn table_name() -> &'static str {
                #table_name
            }

            fn crud_fields() -> Vec<tymigrawr::CrudField> {
                let mut r = Vec::new();
                #(r.push({#crud_fields});)*
                r
            }

            fn as_crud_fields(&self) -> std::collections::HashMap<&str, tymigrawr::Value> {
                std::collections::HashMap::from_iter([
                    #(#as_crud_field_pairs),*
                ])
            }

            fn primary_key_name() -> &'static str {
                #primary_key
            }

            fn primary_key_val(&self) -> tymigrawr::Value {
                #primary_key_val
            }

            fn set_primary_key(&mut self, value: tymigrawr::Value) {
                #set_primary_key
            }

            fn try_from_crud_fields(
                fields: &std::collections::HashMap<&str, tymigrawr::Value>,
            ) -> core::result::Result<Self, tymigrawr::HasCrudFieldsError> {
                #(#from_crud_fields)*
                Ok(Self{
                    #(#field_idents),*
                })
            }
        }
    };

    output.into()
}
