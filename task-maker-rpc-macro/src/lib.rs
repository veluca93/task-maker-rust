use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use proc_macro_error::{abort, proc_macro_error};
use quote::{format_ident, quote};
use syn::{parse_macro_input, FnArg, ItemTrait, PatType, ReturnType, TraitItem};

fn service_impl(input: ItemTrait) -> TokenStream2 {
    if input.items.len() >= 255 {
        abort!(input, "Only less than 255 methods are supported");
    }
    let vis = input.vis;
    let ident = input.ident;
    let ident_server = format_ident!("{}Server", ident);
    let ident_input = format_ident!("{}InputMessage", ident);
    let ident_output = format_ident!("{}OutputMessage", ident);

    let mut input_messages = vec![];
    let mut output_messages = vec![];

    for (idx, method) in input.items.iter().enumerate() {
        let method = match method {
            TraitItem::Method(m) => m,
            _ => {
                abort!(method, "Invalid item in trait, only methods are supported");
            }
        };
        let message_input_ident =
            format_ident!("{}", method.sig.ident.to_string().to_case(Case::UpperCamel));
        if method.sig.inputs.len() < 1 {
            abort!(method, "RPC methods must have at least one argument");
        }
        match &method.sig.inputs[0] {
            FnArg::Receiver(rec) => {
                if rec.mutability.is_some()
                    || rec.reference.is_none()
                    || (rec.reference.is_some() && rec.reference.as_ref().unwrap().1.is_some())
                {
                    abort!(
                        rec,
                        "Methods in RPC services must take &self with no lifetimes"
                    );
                }
                println!("{:?}", rec);
            }
            _ => {
                abort!(
                    method,
                    "Associated functions don't make sense in RPC services"
                );
            }
        }
        let mut args = vec![];
        for arg in method.sig.inputs.iter().skip(1) {
            match &arg {
                FnArg::Typed(PatType { pat, ty, .. }) => {
                    args.push(quote!(#pat: #ty));
                }
                _ => {
                    abort!(arg, "Unexpected receiver");
                }
            }
        }
        input_messages.push(quote!(
            #message_input_ident {
                #(#args),*
            }
        ));
        let message_output_ident =
            format_ident!("{}", method.sig.ident.to_string().to_case(Case::UpperCamel));
        let ret_type = match &method.sig.output {
            ReturnType::Default => quote!(()),
            ReturnType::Type(_, x) => quote!(#x),
        };
        output_messages.push(quote!(
            #message_output_ident(#ret_type)
        ));
    }

    let doc = input.attrs.iter().find(|attr| attr.path.is_ident("doc"));

    quote!(
        #doc
        #vis struct #ident {}
        #[allow(missing_docs)]
        #[derive(Serialize, Deserialize)]
        enum #ident_input {
            #(#input_messages),*
        }
        #[allow(missing_docs)]
        #[derive(Serialize, Deserialize)]
        enum #ident_output {
            #(#output_messages),*
        }
    )
}

#[proc_macro_attribute]
pub fn service(_attr: TokenStream, item: TokenStream) -> TokenStream {
    service_impl(parse_macro_input!(item as ItemTrait)).into()
}
