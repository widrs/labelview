use eyre::{bail, eyre as err, Result};

pub use atrium_api::did_doc::DidDocument;

pub async fn did(handle_or_did: &str) -> Result<String> {
    // most of the lookup logic here is learned from
    // https://github.com/bluesky-social/atproto/tree/main/packages/identity
    if handle_or_did.starts_with("did:") {
        Ok(handle_or_did.to_owned())
    } else {
        if let Some(did) = find_did_in_dns(&format!("_atproto.{handle_or_did}")).await {
            return Ok(did);
        } else if let Some(did) = find_did_in_well_known(handle_or_did).await {
            return Ok(did);
        }
        bail!("could not resolve did from handle");
    }
}

async fn find_did_in_dns(dns_domain: &str) -> Option<String> {
    println!("looking up did via dns TXT...");
    let dns_resolver =
        hickory_resolver::TokioAsyncResolver::tokio(Default::default(), Default::default());
    let lookup = dns_resolver.txt_lookup(dns_domain).await.ok()?;
    for record in lookup.iter() {
        let Some((first, rest)) = record.txt_data().split_first() else {
            continue;
        };
        let Some(after_prefix) = first.strip_prefix("did=".as_bytes()) else {
            continue;
        };
        let mut full_text = Vec::new();
        full_text.extend_from_slice(&after_prefix);
        full_text.extend(rest.into_iter().flatten());
        return String::from_utf8(full_text).ok();
    }
    None
}

async fn find_did_in_well_known(https_domain: &str) -> Option<String> {
    println!("looking up did via dns HTTPS .well-known...");
    let http_client = reqwest::Client::new();
    let response = http_client
        .get(format!("https://{https_domain}/.well-known/atproto-did"))
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .ok()?;
    let content = response.bytes().await.ok()?;
    match std::str::from_utf8(&content) {
        Ok(content) if content.starts_with("did:") => Some(content.to_owned()),
        _ => None,
    }
}

pub async fn did_doc(plc_directory: &str, did: &str) -> Result<DidDocument> {
    let doc: DidDocument = match did.strip_prefix("did:").and_then(|s| s.split_once(':')) {
        Some(("plc", _)) => {
            println!("reading did document from plc directory...");
            let http_client = reqwest::Client::new();
            let response = http_client
                .get(format!("https://{plc_directory}/{did}"))
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|e| err!("error fetching did from plc directory: {e}"))?;
            // parse the json response
            let content = response
                .bytes()
                .await
                .map_err(|e| err!("error reading did from plc directory response: {e}"))?;
            serde_json::from_slice(&content)
                .map_err(|e| err!("error parsing did document from plc directory: {e}"))?
        }
        Some(("web", domain)) => {
            let http_client = reqwest::Client::new();
            let response = http_client
                .get(format!("https://{domain}/.well-known/did.json"))
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|e| err!("error fetching did from .well-known: {e}"))?;
            // parse the json response
            let content = response
                .bytes()
                .await
                .map_err(|e| err!("error reading did from .well-known response: {e}"))?;
            serde_json::from_slice(&content)
                .map_err(|e| err!("error parsing did document from .well-known: {e}"))?
        }
        Some(_) => {
            bail!("unsupported did type");
        }
        None => {
            bail!("not a did");
        }
    };
    if doc.id != did {
        bail!("the fetched did document didn't match the request");
    }
    Ok(doc)
}

pub fn handle_from_doc(doc: &DidDocument) -> Option<&str> {
    doc.also_known_as
        .iter()
        .flatten()
        .find_map(|aka| aka.strip_prefix("at://"))
}

pub fn service_from_doc<'a>(
    doc: &'a DidDocument,
    id_suffix: &str,
    service_type: &str,
) -> Option<&'a str> {
    doc.service.iter().flatten().find_map(|service| {
        if service.id.ends_with(id_suffix) && service.r#type == service_type {
            Some(service.service_endpoint.as_str())
        } else {
            None
        }
    })
}
