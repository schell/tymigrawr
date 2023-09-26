//! Provides derive macros for `tymigrawr::HasCrudFields`.
use quote::quote;
use syn::{
    Attribute, Data, DataStruct, DeriveInput, Fields, FieldsNamed, Ident, Type, WhereClause,
    WherePredicate,
};

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
            let atts = atts
                .iter()
                .filter_map(|att| att.path.get_ident())
                .map(|id| format!("{}", id));
            let mut extras = vec![];
            for att in atts {
                match att.as_str() {
                    "primary_key" => {
                        extras.push(quote! {
                            #ident.primary_key = true;
                        });
                    }
                    _ => {}
                }
            }
            quote! {
                let mut #ident = #ty::field();
                #ident.name = stringify!(#ident);
                #(#extras)*
                #ident
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
        (quote! {stringify!(#ident)}, quote! {self.#ident.into_value()})
    } else {
        (
            quote! {
                compile_error!("must have at least one field")
            },
            quote! {},
        )
    }
}

fn gen_from_crud_fields(idents: &[Ident], tys: &[Type]) -> Vec<proc_macro2::TokenStream> {
    idents
        .iter()
        .zip(tys.iter())
        .map(|(ident, ty)| {
            quote! {
                let #ident = fields
                    .get(stringify!(#ident))
                    .whatever_context(concat!("missing ", stringify!(#ident)))?;
                let #ident = #ty::maybe_from_value(#ident)
                    .whatever_context(concat!("convert ", stringify!(#ident)))?;
            }
        })
        .collect()
}

/// Macro for deriving structs that have normal CRUD-worthy fields.
#[proc_macro_derive(HasCrudFields, attributes(primary_key))]
pub fn derive_crud_fields(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: DeriveInput = syn::parse_macro_input!(input);
    let name = input.ident;
    let (field_idents, field_tys, field_atts) = get_fields(&input.data);
    let mut generics = input.generics;
    {
        /// Adds a `HasCrudFields` constraint on each of the field types.
        fn constrain_field_types(clause: &mut WhereClause, tys: &[Type]) {
            for ty in tys.iter() {
                let where_predicate: WherePredicate =
                    syn::parse_quote!(#ty : tymigrawr::IsCrudField);
                clause.predicates.push(where_predicate);
            }
        }

        let where_clause = generics.make_where_clause();
        constrain_field_types(where_clause, &field_tys)
    }

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let table_name = name.to_string().to_ascii_lowercase();
    let crud_fields = gen_crud_fields(&field_idents, &field_tys, &field_atts);
    let from_crud_fields = gen_from_crud_fields(&field_idents, &field_tys);
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
                    #((stringify!(#field_idents), self.#field_idents.into_value())),*
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
