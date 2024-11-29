use anyhow::{anyhow, bail, Result};
use serde::Deserialize;
use std::io::Write;
use url::Url;

/// https://docs.bsky.app/docs/api/com-atproto-repo-describe-repo
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DidDescription {
    pub handle: String,
    pub did: String,
    pub handle_is_correct: bool,
    pub did_doc: atrium_api::did_doc::DidDocument,
    #[allow(dead_code)]
    pub collections: Vec<String>,
}
#[derive(Deserialize)]
struct ErrorResponse {
    error: String,
    message: String,
}

pub async fn did(entryway_service: &str, handle_or_did: &str) -> Result<DidDescription> {
    let http_client = reqwest::Client::new();
    let did_lookup = http_client
        .get(format!(
            "https://{entryway_service}/xrpc/com.atproto.repo.describeRepo"
        ))
        .query(&[("repo", handle_or_did)])
        .send()
        .await
        .map_err(|e| anyhow!("error fetching did: {e}"))?;

    // parse the json response
    let did_lookup = did_lookup
        .bytes()
        .await
        .map_err(|e| anyhow!("error reading did from response: {e}"))?;
    serde_json::from_slice(&did_lookup).map_err(|mut e| {
        if e.is_data() {
            // problem with content, try to parse as an error instead
            match serde_json::from_slice(&did_lookup) {
                Ok(ErrorResponse { error, message }) => {
                    return anyhow!("error from entryway: {error}: {message}");
                }
                Err(parse_err_2) => e = parse_err_2,
            }
        }
        // problem with reading or bad json data
        anyhow!("error parsing did document from entryway: {e}")
    })
}

pub async fn labeler_by_handle(entryway_service: &str, handle_or_did: &str) -> Result<String> {
    // read the did document from the entryway to get the service endpoints for the labeler
    print!("looking up did...");
    std::io::stdout().flush()?;
    let desc = did(entryway_service, handle_or_did).await?;
    println!("ok");

    // read the handle, did, and pds & labeler endpoint urls from the response
    let DidDescription {
        handle,
        did,
        handle_is_correct,
        ..
    } = &desc;
    let pds = desc.did_doc.service.iter().flatten().find_map(|service| {
        if service.id.ends_with("#atproto_pds") && service.r#type == "AtprotoPersonalDataServer" {
            Some(service.service_endpoint.clone())
        } else {
            None
        }
    });
    let labeler = desc.did_doc.service.iter().flatten().find_map(|service| {
        if service.id.ends_with("#atproto_labeler") && service.r#type == "AtprotoLabeler" {
            Some(service.service_endpoint.clone())
        } else {
            None
        }
    });
    println!("handle: {handle}");
    println!("did:    {did}");
    println!("handle is correct: {handle_is_correct:?}");
    println!();
    let pds_text = pds.as_deref().unwrap_or("(no pds endpoint defined)");
    let labeler_text = labeler
        .as_deref()
        .unwrap_or("(no labeler endpoint defined)");
    println!("pds:     {pds_text}");
    println!("labeler: {labeler_text}");
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
