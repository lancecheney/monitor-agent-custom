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
    /// V6 runs second, after every v4 carrier finished, and only on machines
    /// with a default IPv6 route; `serde(default)` keeps older hubs compatible.
    #[serde(default)]
    pub ipv6: bool,
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
    pub stack: &'static str,
    pub carrier: &'static str,
    pub carrier_label: &'static str,
    pub target: String,
    pub hops: Vec<RouteHop>,
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
    let mut items = Vec::with_capacity(6);
    // V4 always runs; v6 only when asked for, after v4 finished, and only on a
    // machine with a default IPv6 route -- otherwise the whole v6 phase is
    // skipped rather than reported as three failures.
    let stacks: &[(&str, &str)] =
        if request.ipv6 && has_ipv6_route().await { &[("-4", "v4"), ("-6", "v6")] } else { &[("-4", "v4")] };
    for (flag, stack) in stacks {
        for (carrier, label) in CARRIERS {
            let target = format!("{code}-{carrier}-{stack}.ip.zstaticcdn.com");
            let mut sample = None;
            let mut errors = Vec::new();
            let mut success = 0;
            for _ in 0..request.rounds {
                match trace(binary, flag, stack, carrier, label, &target).await {
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
                    stack,
                    carrier,
                    carrier_label: label,
                    target,
                    hops: Vec::new(),
                    success: 0,
                    rounds: request.rounds,
                    error: Some(
                        errors.pop().unwrap_or_else(|| "未知错误".into()).chars().take(240).collect(),
                    ),
                });
            items.push(item);
        }
    }
    Ok(ResultMessage {
        request_id: request.request_id,
        province: request.province,
        rounds: request.rounds,
        items,
    })
}

/// A default IPv6 route is what "this machine has v6" means here; the trace
/// itself would only fail hop by hop, which reads as a broken line, not as
/// "no v6". Machines without `ip` count as v4-only.
async fn has_ipv6_route() -> bool {
    Command::new("ip")
        .args(["-6", "route", "show", "default"])
        .stdin(Stdio::null())
        .output()
        .await
        .map(|output| output.status.success() && !String::from_utf8_lossy(&output.stdout).trim().is_empty())
        .unwrap_or(false)
}

async fn trace(
    binary: &str,
    flag: &str,
    stack: &'static str,
    carrier: &'static str,
    label: &'static str,
    target: &str,
) -> Result<RouteItem> {
    let output = tokio::time::timeout(
        TRACE_TIMEOUT,
        Command::new(binary)
            .args([
                flag,
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
    Ok(RouteItem {
        stack,
        carrier,
        carrier_label: label,
        target: target.into(),
        hops: measured_hops(&hops),
        success: 1,
        rounds: 1,
        error: None,
    })
}

/// One measured hop as the hub receives it: the address that answered, every AS
/// number reported for it, and the place NextTrace geolocated it to, in trace
/// order. The hub owns the naming.
#[derive(Clone, Debug, Serialize)]
pub struct RouteHop {
    pub ip: String,
    pub asns: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub region: String,
}

fn measured_hops(hops: &[Value]) -> Vec<RouteHop> {
    hops.iter()
        .filter_map(|hop| {
            let ip = hop_ips(hop).into_iter().next().unwrap_or_default();
            let asns = hop_asns(hop);
            (ip.len() >= 7 || !asns.is_empty()).then_some(RouteHop { ip, asns, region: hop_region(hop) })
        })
        .collect()
}

/// The place a hop answered from, as the panel prints it: a Chinese province
/// inside the mainland (浙江), the region itself for the likes of 香港/台湾
/// that NextTrace files under country 中国, and the country everywhere else
/// (美国 rather than 加利福尼亚州). Empty when nothing geolocated.
fn hop_region(hop: &Value) -> String {
    fn field(geo: &Value, key: &str) -> String {
        geo.get(key).and_then(Value::as_str).unwrap_or("").trim().to_owned()
    }
    let geo = hop.get("Geo").unwrap_or(&Value::Null);
    let region = {
        let country = field(geo, "country");
        let prov = field(geo, "prov");
        if country == "中国" && !prov.is_empty() {
            prov
        } else if !country.is_empty() {
            country
        } else {
            field(geo, "city")
        }
    };
    region.chars().take(16).collect()
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

    /// The agent ships raw hops; the hub decides the name. A NextTrace JSON
    /// payload's successful candidates collapse to one hop each, keeping the
    /// tracer's order and every AS number seen there.
    #[test]
    fn measured_hops_keep_order_and_every_as_number() {
        let payload = serde_json::json!({"Hops": [
            [{"Address": "45.207.58.10", "Geo": {"asnumber": "205548"}, "Success": true}],
            [{"Address": "210.171.224.1", "Geo": {"asnumber": "2497"}, "Success": true}],
            [{"Address": "202.97.96.1", "Geo": {"asnumber": "4134"}, "Success": false},
             {"Address": "202.97.96.1", "Geo": {"asnumber": "4134"}, "Success": true}],
        ]});
        let hops = measured_hops(&successful_hops(&payload));
        assert_eq!(hops.len(), 3);
        assert_eq!(hops[0].ip, "45.207.58.10");
        assert_eq!(hops[0].asns, vec!["205548"]);
        assert_eq!(hops[2].ip, "202.97.96.1", "the successful candidate is the one kept");
        assert_eq!(hops[2].asns, vec!["4134"]);
    }

    /// A hop the tracer resolved only as an AS (no usable address) still ships,
    /// so the hub's path rendering loses nothing; a hop with neither is dropped.
    #[test]
    fn as_only_hops_ship_and_empty_ones_do_not() {
        let payload = serde_json::json!({"Hops": [
            [{"Address": "", "Geo": {"asnumber": "4134"}, "Success": true}],
            [{"Address": "203.0.113.1", "Success": true}],
        ]});
        let hops = measured_hops(&successful_hops(&payload));
        assert_eq!(hops.len(), 2);
        assert_eq!(hops[0].asns, vec!["4134"]);
        assert_eq!(hops[1].ip, "203.0.113.1");
    }

    /// A mainland hop names its province, 香港 keeps its region although
    /// NextTrace files it under country 中国, and everywhere else the country
    /// is the label -- the panel prints 台湾-香港-浙江 style chains, not
    /// 加利福尼亚州.
    #[test]
    fn regions_prefer_cn_province_then_region_then_country() {
        let payload = serde_json::json!({"Hops": [
            [{"Address": "202.97.96.1", "Geo": {"country": "中国", "prov": "浙江", "city": "杭州"}, "Success": true}],
            [{"Address": "45.207.58.10", "Geo": {"country": "中国", "prov": "香港"}, "Success": true}],
            [{"Address": "38.55.108.1", "Geo": {"country": "美国", "prov": "加利福尼亚州", "city": "洛杉矶"}, "Success": true}],
            [{"Address": "203.0.113.7", "Geo": {}, "Success": true}],
        ]});
        let hops = measured_hops(&successful_hops(&payload));
        assert_eq!(hops[0].region, "浙江");
        assert_eq!(hops[1].region, "香港");
        assert_eq!(hops[2].region, "美国");
        assert_eq!(hops[3].region, "");
    }

    /// Hubs that predate the v6 flag keep working: `ipv6` defaults to false and
    /// such a request traces v4 only.
    #[test]
    fn older_hubs_omit_ipv6_and_still_parse() {
        let request: Request = serde_json::from_value(serde_json::json!({
            "request_id": "req-1", "province": "浙江", "rounds": 1
        }))
        .unwrap();
        assert!(!request.ipv6);
    }
}
