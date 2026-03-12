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

/// Returns `true` if the field's attributes contain `#[json_text]`.
fn has_json_text_attr(atts: &[Attribute]) -> bool {
    atts.iter().any(|att| {
        att.path
            .get_ident()
            .map(|id| id == "json_text")
            .unwrap_or(false)
    })
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
            let is_json = has_json_text_attr(atts);
            let att_strs = atts
                .iter()
                .filter_map(|att| att.path.get_ident())
                .map(|id| format!("{}", id));
            let mut extras = vec![];
            for att in att_strs {
                #[expect(
                    clippy::single_match,
                    reason = "We keep this here for extensibility sake"
                )]
                match att.as_str() {
                    "primary_key" => {
                        extras.push(quote! {
                            #ident.primary_key = true;
                        });
                    }
                    _ => {}
                }
            }
            if is_json {
                // json_text fields are stored as TEXT — the field type does not
                // implement IsCrudField, so we build the CrudField directly.
                let nullable = is_option_type(ty);
                quote! {
                    let mut #ident = tymigrawr::CrudField {
                        ty: tymigrawr::ValueType::String,
                        nullable: #nullable,
                        ..Default::default()
                    };
                    #ident.name = stringify!(#ident);
                    #(#extras)*
                    #ident
                }
            } else {
                quote! {
                    let mut #ident = <#ty>::field();
                    #ident.name = stringify!(#ident);
                    #(#extras)*
                    #ident
                }
            }
        })
        .collect()
}

fn get_primary_key(
    idents: &[Ident],
    atts: &[Vec<Attribute>],
) -> (proc_macro2::TokenStream, proc_macro2::TokenStream) {
    let mut keys = idents.iter().zip(atts).filter_map(|(ident, atts)| {
        for att in atts.iter() {
            let att = att.path.get_ident()?;
            if format!("{}", att) == "primary_key" {
                return Some(ident.clone());
            }
        }
        None
    });
    let may_ident = if let Some(ident) = keys.next() {
        Some(ident)
    } else {
        idents.first().cloned()
    };

    if let Some(ident) = may_ident {
        (
            quote! {stringify!(#ident)},
            quote! {self.#ident.into_value()},
        )
    } else {
        (
            quote! {
                compile_error!("must have at least one field")
            },
            quote! {},
        )
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
            let is_json = has_json_text_attr(field_atts);
            let is_option = is_option_type(ty);

            if is_json && is_option {
                // Option<T> + json_text: tolerate NULL/missing, deserialize
                // the inner type from JSON when present.
                let deser_msg = format!("deserialize json_text {}", ident);
                let expected_msg = format!("expected string for json_text {}", ident);
                quote! {
                    let #ident = match fields.get(stringify!(#ident)) {
                        Some(tymigrawr::Value::String(s)) => {
                            Some(
                                serde_json::from_str(s)
                                    .whatever_context(#deser_msg)?,
                            )
                        }
                        Some(tymigrawr::Value::None) | None => None,
                        _ => snafu::whatever!(#expected_msg),
                    };
                }
            } else if is_json {
                // Required json_text field: deserialize from Value::String.
                let missing_msg = format!("missing {}", ident);
                let deser_msg = format!("deserialize json_text {}", ident);
                let expected_msg = format!("expected string for json_text {}", ident);
                quote! {
                    let #ident = match fields
                        .get(stringify!(#ident))
                        .whatever_context(#missing_msg)?
                    {
                        tymigrawr::Value::String(s) => {
                            serde_json::from_str::<#ty>(s)
                                .whatever_context(#deser_msg)?
                        }
                        _ => snafu::whatever!(#expected_msg),
                    };
                }
            } else if is_option {
                quote! {
                    let #ident = match fields.get(stringify!(#ident)) {
                        Some(v) => <#ty>::maybe_from_value(v),
                        None => None,
                    };
                }
            } else {
                quote! {
                    let #ident = fields
                        .get(stringify!(#ident))
                        .whatever_context(concat!("missing ", stringify!(#ident)))?;
                    let #ident = <#ty>::maybe_from_value(#ident)
                        .whatever_context(concat!("convert ", stringify!(#ident)))?;
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
        .map(|(ident, (ty, field_atts))| {
            let is_json = has_json_text_attr(field_atts);
            let is_option = is_option_type(ty);
            if is_json && is_option {
                // Option<T> json_text: None → Value::None, Some(v) → JSON string
                quote! {
                    (
                        stringify!(#ident),
                        match &self.#ident {
                            Some(inner) => tymigrawr::Value::String(
                                serde_json::to_string(inner)
                                    .expect(concat!(
                                        "failed to serialize json_text field ",
                                        stringify!(#ident),
                                    )),
                            ),
                            None => tymigrawr::Value::None,
                        },
                    )
                }
            } else if is_json {
                quote! {
                    (
                        stringify!(#ident),
                        tymigrawr::Value::String(
                            serde_json::to_string(&self.#ident)
                                .expect(concat!(
                                    "failed to serialize json_text field ",
                                    stringify!(#ident),
                                )),
                        ),
                    )
                }
            } else {
                quote! {
                    (stringify!(#ident), self.#ident.into_value())
                }
            }
        })
        .collect()
}

/// Macro for deriving structs that have normal CRUD-worthy fields.
#[proc_macro_derive(HasCrudFields, attributes(primary_key, json_text))]
pub fn derive_crud_fields(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: DeriveInput = syn::parse_macro_input!(input);
    let name = input.ident;
    let (field_idents, field_tys, field_atts) = get_fields(&input.data);
    let mut generics = input.generics;
    {
        /// Adds trait constraints on each of the field types.
        ///
        /// Normal fields get `IsCrudField`; `#[json_text]` fields get
        /// `serde::Serialize + serde::de::DeserializeOwned`.
        fn constrain_field_types(clause: &mut WhereClause, tys: &[Type], atts: &[Vec<Attribute>]) {
            for (ty, field_atts) in tys.iter().zip(atts) {
                if has_json_text_attr(field_atts) {
                    let where_predicate: WherePredicate =
                        syn::parse_quote!(#ty : serde::Serialize + serde::de::DeserializeOwned);
                    clause.predicates.push(where_predicate);
                } else {
                    let where_predicate: WherePredicate =
                        syn::parse_quote!(#ty : tymigrawr::IsCrudField);
                    clause.predicates.push(where_predicate);
                }
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
    let (primary_key, primary_key_val) = get_primary_key(&field_idents, &field_atts);
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

            fn try_from_crud_fields(
                fields: &std::collections::HashMap<&str, tymigrawr::Value>,
            ) -> Result<Self, snafu::Whatever> {
                #(#from_crud_fields)*
                Ok(Self{
                    #(#field_idents),*
                })
            }
        }
    };

    output.into()
}
