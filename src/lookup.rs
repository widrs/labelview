use crate::db;
use anyhow::{anyhow, bail, Result};
use std::io::Write;
use url::Url;

pub use atrium_api::did_doc::DidDocument;

async fn did(handle_or_did: &str) -> Result<String> {
    if handle_or_did.starts_with("did:") {
        Ok(handle_or_did.to_owned())
    } else {
        if let Some(did) = find_did_in_dns(&format!("_atproto.{handle_or_did}")).await {
            return Ok(did);
        } else if let Some(did) = find_did_in_well_known(handle_or_did).await {
            return Ok(did);
        }
        bail!("could not resolve did from handle")
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

async fn did_doc(plc_directory: &str, did: &str) -> Result<DidDocument> {
    let doc: DidDocument = match did.strip_prefix("did:").and_then(|s| s.split_once(':')) {
        Some(("plc", _)) => {
            println!("reading did document from plc directory...");
            let http_client = reqwest::Client::new();
            let response = http_client
                .get(format!("https://{plc_directory}/{did}"))
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|e| anyhow!("error fetching did from plc directory: {e}"))?;
            // parse the json response
            let content = response
                .bytes()
                .await
                .map_err(|e| anyhow!("error reading did from plc directory response: {e}"))?;
            serde_json::from_slice(&content)
                .map_err(|e| anyhow!("error parsing did document from plc directory: {e}"))?
        }
        Some(("web", domain)) => {
            let http_client = reqwest::Client::new();
            let response = http_client
                .get(format!("https://{domain}/.well-known/did.json"))
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|e| anyhow!("error fetching did from .well-known: {e}"))?;
            // parse the json response
            let content = response
                .bytes()
                .await
                .map_err(|e| anyhow!("error reading did from .well-known response: {e}"))?;
            serde_json::from_slice(&content)
                .map_err(|e| anyhow!("error parsing did document from .well-known: {e}"))?
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

fn handle_from_doc(doc: &DidDocument) -> Option<&str> {
    doc.also_known_as
        .iter()
        .flatten()
        .find_map(|aka| aka.strip_prefix("at://"))
}

pub async fn labeler_by_handle(
    store: &mut db::Connection,
    pds_directory: &str,
    handle_or_did: &str,
) -> Result<String> {
    // read the did document from the entryway to get the service endpoints for the labeler
    println!("looking up did...");
    std::io::stdout().flush()?;
    let did = did(handle_or_did).await?;
    let doc = did_doc(pds_directory, &did).await?;
    let handle = handle_from_doc(&doc);
    let handle_text = handle.unwrap_or("(no handle listed in did)");
    // read the handle, did, and pds & labeler endpoint urls from the response
    let pds = doc.service.iter().flatten().find_map(|service| {
        if service.id.ends_with("#atproto_pds") && service.r#type == "AtprotoPersonalDataServer" {
            Some(service.service_endpoint.clone())
        } else {
            None
        }
    });
    let labeler = doc.service.iter().flatten().find_map(|service| {
        if service.id.ends_with("#atproto_labeler") && service.r#type == "AtprotoLabeler" {
            Some(service.service_endpoint.clone())
        } else {
            None
        }
    });

    println!();
    println!("handle: {handle_text}");
    println!("did:    {did}");
    println!();
    let pds_text = pds.as_deref().unwrap_or("(no pds endpoint defined)");
    let labeler_text = labeler
        .as_deref()
        .unwrap_or("(no labeler endpoint defined)");
    println!("pds:     {pds_text}");
    println!("labeler: {labeler_text}");

    if let Some(handle) = handle {
        db::witness_handle_did(store, handle, &did)?;
    }
    let Some(labeler) = labeler else {
        bail!("that entity doesn't seem to be a labeler.");
    };

    let labeler_url = Url::parse(&labeler)
        .map_err(|e| anyhow!("could not parse labeler endpoint as url: {e}"))?;
    if labeler_url.domain().is_none() {
        bail!("labeler endpoint url does not seem to specify a domain");
    }
    Ok(labeler)
}
