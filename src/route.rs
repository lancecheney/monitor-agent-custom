use std::collections::HashSet;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::process::Command;

const TRACE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Deserialize)]
pub struct Request {
    pub request_id: String,
    pub province: String,
    pub rounds: u8,
}

#[derive(Debug, Serialize)]
pub struct ResultMessage {
    pub request_id: String,
    pub province: String,
    pub rounds: u8,
    pub items: Vec<RouteItem>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RouteItem {
    pub carrier: &'static str,
    pub carrier_label: &'static str,
    pub target: String,
    pub line: String,
    pub path: String,
    pub success: u8,
    pub rounds: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

const PROVINCES: [(&str, &str); 31] = [
    ("北京", "bj"),
    ("天津", "tj"),
    ("河北", "he"),
    ("山西", "sx"),
    ("内蒙古", "nm"),
    ("辽宁", "ln"),
    ("吉林", "jl"),
    ("黑龙江", "hl"),
    ("上海", "sh"),
    ("江苏", "js"),
    ("浙江", "zj"),
    ("安徽", "ah"),
    ("福建", "fj"),
    ("江西", "jx"),
    ("山东", "sd"),
    ("河南", "ha"),
    ("湖北", "hb"),
    ("湖南", "hn"),
    ("广东", "gd"),
    ("广西", "gx"),
    ("海南", "hi"),
    ("重庆", "cq"),
    ("四川", "sc"),
    ("贵州", "gz"),
    ("云南", "yn"),
    ("西藏", "xz"),
    ("陕西", "sn"),
    ("甘肃", "gs"),
    ("青海", "qh"),
    ("宁夏", "nx"),
    ("新疆", "xj"),
];

const CARRIERS: [(&str, &str); 3] = [("ct", "中国电信"), ("cu", "中国联通"), ("cm", "中国移动")];

pub fn valid_province(value: &str) -> bool {
    PROVINCES.iter().any(|(name, _)| *name == value)
}

pub async fn run(request: Request) -> Result<ResultMessage> {
    if request.request_id.is_empty() || request.request_id.len() > 64 || !valid_province(&request.province) {
        bail!("invalid route-test request");
    }
    if !(1..=3).contains(&request.rounds) {
        bail!("route-test rounds must be between 1 and 3");
    }
    let code = PROVINCES.iter().find(|(name, _)| *name == request.province).unwrap().1;
    let binary = ["/opt/monitor/nexttrace", "/usr/local/bin/nexttrace"]
        .into_iter()
        .find(|path| Path::new(path).is_file())
        .context("NextTrace is not installed")?;
    let mut items = Vec::with_capacity(3);
    for (carrier, label) in CARRIERS {
        let target = format!("{code}-{carrier}-v4.ip.zstaticcdn.com");
        let mut sample = None;
        let mut errors = Vec::new();
        let mut success = 0;
        for _ in 0..request.rounds {
            match trace(binary, carrier, label, &target).await {
                Ok(item) => {
                    success += 1;
                    sample.get_or_insert(item);
                }
                Err(error) => errors.push(error.to_string()),
            }
        }
        let item = sample
            .map(|mut item| {
                item.success = success;
                item.rounds = request.rounds;
                item
            })
            .unwrap_or(RouteItem {
                carrier,
                carrier_label: label,
                target,
                line: "测试失败".into(),
                path: String::new(),
                success: 0,
                rounds: request.rounds,
                error: Some(errors.pop().unwrap_or_else(|| "未知错误".into()).chars().take(240).collect()),
            });
        items.push(item);
    }
    Ok(ResultMessage {
        request_id: request.request_id,
        province: request.province,
        rounds: request.rounds,
        items,
    })
}

async fn trace(binary: &str, carrier: &'static str, label: &'static str, target: &str) -> Result<RouteItem> {
    let output = tokio::time::timeout(
        TRACE_TIMEOUT,
        Command::new(binary)
            .args([
                "-4",
                "-q",
                "3",
                "--parallel-requests",
                "1",
                "-m",
                "25",
                "-n",
                "--json",
                "--no-color",
                target,
            ])
            .stdin(Stdio::null())
            .output(),
    )
    .await
    .context("NextTrace timed out")??;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        bail!("{}", if stderr.trim().is_empty() { stdout.trim() } else { stderr.trim() });
    }
    let payload: Value = serde_json::from_slice(&output.stdout).context("invalid NextTrace JSON")?;
    let hops = successful_hops(&payload);
    let mut known = Vec::new();
    for hop in &hops {
        for asn in hop_asns(hop) {
            if !known.contains(&asn) {
                known.push(asn);
            }
        }
    }
    let line = classify(carrier, &hops).unwrap_or_else(|| "未识别".into());
    Ok(RouteItem {
        carrier,
        carrier_label: label,
        target: target.into(),
        line,
        path: known.iter().map(|asn| format!("AS{asn}")).collect::<Vec<_>>().join(" → "),
        success: 1,
        rounds: 1,
        error: None,
    })
}

fn successful_hops(payload: &Value) -> Vec<Value> {
    payload
        .get("Hops")
        .or_else(|| payload.get("hops"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|group| {
            let candidates: Vec<&Value> =
                group.as_array().map(|v| v.iter().collect()).unwrap_or_else(|| vec![group]);
            candidates
                .into_iter()
                .find(|item| {
                    item.as_object().is_some()
                        && item
                            .get("Success")
                            .or_else(|| item.get("success"))
                            .and_then(Value::as_bool)
                            .unwrap_or(true)
                })
                .cloned()
        })
        .collect()
}

fn hop_asns(hop: &Value) -> Vec<String> {
    let text = hop.to_string();
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut index = 0;
    while index + 2 < bytes.len() {
        if bytes[index].eq_ignore_ascii_case(&b'a') && bytes[index + 1].eq_ignore_ascii_case(&b's') {
            let mut start = index + 2;
            while start < bytes.len() && bytes[start].is_ascii_whitespace() {
                start += 1;
            }
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() && end - start < 10 {
                end += 1;
            }
            if end - start >= 2 {
                out.push(text[start..end].to_owned());
            }
            index = end;
        } else {
            index += 1;
        }
    }
    for key in ["asn", "asnumber"] {
        collect_numeric_key(hop, key, &mut out);
    }
    out.retain(|asn| asn.len() >= 2 && asn.len() <= 10);
    let mut seen = HashSet::new();
    out.retain(|asn| seen.insert(asn.clone()));
    out
}

fn collect_numeric_key(value: &Value, key: &str, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (name, value) in map {
                if name.eq_ignore_ascii_case(key) {
                    let text = value.as_str().map(str::to_owned).unwrap_or_else(|| value.to_string());
                    let digits: String = text.chars().filter(|c| c.is_ascii_digit()).collect();
                    if !digits.is_empty() {
                        out.push(digits);
                    }
                }
                collect_numeric_key(value, key, out);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_numeric_key(value, key, out);
            }
        }
        _ => {}
    }
}

fn hop_ips(hop: &Value) -> Vec<String> {
    let text = hop.to_string();
    text.split(|c: char| !(c.is_ascii_digit() || c == '.'))
        .filter(|candidate| candidate.matches('.').count() == 3)
        .filter(|candidate| candidate.parse::<std::net::Ipv4Addr>().is_ok())
        .map(str::to_owned)
        .collect()
}

fn inferred(ip: &str) -> Option<&'static str> {
    let entries = [
        ("4809", &["59.43."][..]),
        ("23764", &["203.22.182.", "203.22.178.", "203.22.179.", "203.128.224.", "69.194."]),
        ("4134", &["202.97.", "202.96.", "219.141.", "219.142.", "106.37."]),
        ("4837", &["219.158."]),
        ("9929", &["210.14.", "210.51.", "210.78.", "218.105."]),
        (
            "10099",
            &[
                "103.214.",
                "103.228.68.",
                "103.239.176.",
                "118.26.151.",
                "162.245.124.",
                "202.77.23.",
                "203.160.66.",
                "203.160.75.",
            ],
        ),
        ("58807", &["223.120.", "223.119."]),
        ("9808", &["221.183.", "111.24.", "111.13."]),
    ];
    for (asn, prefixes) in entries {
        if prefixes.iter().any(|prefix| ip.starts_with(prefix)) {
            return Some(asn);
        }
    }
    let octets: Vec<_> = ip.split('.').collect();
    if octets.len() == 4
        && octets[0] == "162"
        && octets[1] == "219"
        && matches!(octets[2].parse::<u8>(), Ok(32..=39 | 85))
    {
        return Some("10099");
    }
    None
}

fn records(hops: &[Value]) -> Vec<(Vec<String>, HashSet<String>)> {
    hops.iter()
        .map(|hop| {
            let ips = hop_ips(hop);
            let mut asns: HashSet<String> = hop_asns(hop).into_iter().collect();
            for ip in &ips {
                if let Some(asn) = inferred(ip) {
                    asns.insert(asn.into());
                }
            }
            (ips, asns)
        })
        .collect()
}

fn classify(carrier: &str, hops: &[Value]) -> Option<String> {
    let records = records(hops);
    match carrier {
        "ct" => {
            let first = records.iter().position(|(ips, asns)| {
                asns.contains("4809") || ips.iter().any(|ip| ip.starts_with("59.43."))
            })?;
            for (index, (ips, _)) in records.iter().enumerate().skip(first) {
                if ips.iter().any(|ip| ip.starts_with("59.43.245."))
                    && records.iter().skip(index + 1).any(|(later_ips, later_asns)| {
                        later_asns.contains("4134")
                            || later_asns.contains("4847")
                            || later_ips.iter().any(|ip| {
                                ["202.97.", "202.96.", "219.141.", "219.142.", "106.37."]
                                    .iter()
                                    .any(|prefix| ip.starts_with(prefix))
                            })
                    })
                {
                    return Some("CN2GT".into());
                }
            }
            if records.iter().any(|(ips, asns)| {
                asns.contains("23764")
                    || ips.iter().any(|ip| {
                        ["203.22.182.", "203.22.178.", "203.22.179.", "203.128.224.", "69.194."]
                            .iter()
                            .any(|prefix| ip.starts_with(prefix))
                    })
            }) {
                Some("CTGGIA".into())
            } else {
                Some("CN2GIA".into())
            }
        }
        "cu" => {
            let cug = records.iter().position(|(ips, asns)| {
                asns.contains("10099") || ips.iter().any(|ip| inferred(ip) == Some("10099"))
            });
            if let Some(index) = cug {
                let after = &records[index + 1..];
                if after.iter().any(|(_, a)| a.contains("9929")) {
                    Some("CUG+9929".into())
                } else if after.iter().any(|(_, a)| {
                    ["4837", "4808", "17816", "135061", "136958", "140979"].iter().any(|v| a.contains(*v))
                }) {
                    Some("CUG+4837".into())
                } else {
                    Some("CUG".into())
                }
            } else if records.iter().any(|(_, a)| a.contains("9929")) {
                Some("9929".into())
            } else if records.iter().any(|(_, a)| {
                ["4837", "4808", "17816", "135061", "136958", "140979"].iter().any(|v| a.contains(*v))
            }) {
                Some("4837".into())
            } else {
                None
            }
        }
        "cm" => {
            let cmin2 = records.iter().any(|(_, a)| a.contains("58807"));
            let cmi = records.iter().any(|(_, a)| {
                ["58453", "9808", "56040", "56041", "56042", "56044", "56045", "56046", "56047", "56048"]
                    .iter()
                    .any(|v| a.contains(*v))
            });
            match (cmin2, cmi) {
                (true, true) => Some("CMIN2+CMI".into()),
                (true, false) => Some("CMIN2".into()),
                (false, true) => Some("CMI".into()),
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hops(values: &[(&str, &str)]) -> Vec<Value> {
        values
            .iter()
            .map(|(ip, asn)| serde_json::json!({"Address": ip, "Geo": {"asnumber": asn}, "Success": true}))
            .collect()
    }

    #[test]
    fn province_targets_are_fixed() {
        assert!(valid_province("浙江"));
        assert!(!valid_province("任意地址"));
    }

    #[test]
    fn classifies_three_carriers_from_ordered_paths() {
        assert_eq!(
            classify("ct", &hops(&[("59.43.1.1", "4809"), ("203.0.113.1", "4134")])),
            Some("CN2GIA".into())
        );
        assert_eq!(
            classify("cu", &hops(&[("103.214.1.1", "10099"), ("210.14.1.1", "9929")])),
            Some("CUG+9929".into())
        );
        assert_eq!(
            classify("cm", &hops(&[("223.120.1.1", "58807"), ("221.183.1.1", "9808")])),
            Some("CMIN2+CMI".into())
        );
    }
}
