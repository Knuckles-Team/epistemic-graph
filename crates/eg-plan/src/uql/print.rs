//! Plan / program → canonical UQL (UQL-06), and the canonicalization that makes the
//! round trip exact.
//!
//! Single ops and linear plans are printed by eg-types ([`eg_types::wire::uql_op`],
//! [`eg_types::wire::Plan::to_uql`]) where every variant's feature gate is visible. This
//! module adds what needs the parser: [`canonicalize`] (the one encoding per meaning the
//! parser emits) and [`dag_to_uql`] (a `LET … FROM/JOIN` program for a DAG).
//!
//! Contract: for every plan `p` the printer accepts, `parse(print(p)) == canonicalize(p)`.

use std::collections::BTreeMap;

use eg_types::wire::{
    uql_ident, uql_op, CmpOp, EdgeDir, Op, Plan, Pred, PredLiteral, UqlPrintCode, UqlPrintError,
};

use super::{parse_statement, Annotations, Body, DagNode, Mode, Params, Statement};

/// The canonical form of `plan`: the encoding the parser produces for its meaning.
pub fn canonicalize(plan: &Plan) -> Plan {
    Plan::new(plan.ops.iter().map(canonical_op).collect())
}

fn canonical_op(op: &Op) -> Op {
    match op {
        Op::Filter { preds } => Op::Filter {
            preds: preds.iter().map(canonical_pred).collect(),
        },
        Op::DecisionScan { preds } => Op::DecisionScan {
            preds: preds.iter().map(canonical_pred).collect(),
        },
        Op::Expand {
            rel,
            dir,
            min,
            max,
            edge_preds,
        } => match (rel, dir, edge_preds.is_empty()) {
            (Some(rel), EdgeDir::Out, true) => Op::Traverse {
                rel: rel.clone(),
                min: *min,
                max: *max,
            },
            _ => Op::Expand {
                rel: rel.clone(),
                dir: *dir,
                min: *min,
                max: *max,
                edge_preds: edge_preds.iter().map(canonical_pred).collect(),
            },
        },
        Op::Propagate {
            model,
            rel,
            dir,
            edge_preds,
            hops,
            default_transmission,
        } => Op::Propagate {
            model: *model,
            rel: rel.clone(),
            dir: *dir,
            edge_preds: edge_preds.iter().map(canonical_pred).collect(),
            hops: *hops,
            default_transmission: *default_transmission,
        },
        #[cfg(feature = "text")]
        Op::FuseRrf { branches, k } => Op::FuseRrf {
            branches: branches
                .iter()
                .map(|b| b.iter().map(canonical_op).collect())
                .collect(),
            k: *k,
        },
        other => other.clone(),
    }
}

/// The canonical form of one predicate.
pub fn canonical_pred(pred: &Pred) -> Pred {
    match pred {
        Pred::Cmp { prop, op, value } => canonical_cmp(prop, *op, value),
        Pred::And { preds } => connective(preds, |preds| Pred::And { preds }),
        Pred::Or { preds } => connective(preds, |preds| Pred::Or { preds }),
        Pred::Not { pred } => Pred::Not {
            pred: Box::new(canonical_pred(pred)),
        },
        other => other.clone(),
    }
}

fn canonical_cmp(prop: &str, op: CmpOp, value: &PredLiteral) -> Pred {
    let prop = prop.to_string();
    match (op, value) {
        (CmpOp::Gt, PredLiteral::Num(n)) => Pred::GtNum { prop, n: *n },
        (CmpOp::Lt, PredLiteral::Num(n)) => Pred::LtNum { prop, n: *n },
        (CmpOp::Eq, PredLiteral::Str(s)) => Pred::Eq {
            prop,
            value: s.clone(),
        },
        (op, value) => Pred::Cmp {
            prop,
            op,
            value: value.clone(),
        },
    }
}

/// A one-member connective is its member.
fn connective(preds: &[Pred], build: impl Fn(Vec<Pred>) -> Pred) -> Pred {
    let mut preds: Vec<Pred> = preds.iter().map(canonical_pred).collect();
    if preds.len() == 1 {
        preds.remove(0)
    } else {
        build(preds)
    }
}

/// Canonical UQL for a whole statement.
pub fn statement_to_uql(stmt: &Statement) -> Result<String, UqlPrintError> {
    let body = match &stmt.body {
        Body::Pipeline(plan) => plan.to_uql()?,
        Body::Dag(nodes) => dag_to_uql(nodes)?,
    };
    let body = format!("{body}{}", annotations_to_uql(&stmt.annotations));
    let body = if let Some(hint) = &stmt.federation_budget {
        let mut limits = Vec::new();
        if let Some(n) = hint.requests {
            limits.push(format!("REQUESTS {n}"));
        }
        if let Some(n) = hint.rows {
            limits.push(format!("ROWS {n}"));
        }
        if let Some(n) = hint.bind_keys {
            limits.push(format!("BIND_KEYS {n}"));
        }
        if let Some(n) = hint.wall_ms {
            limits.push(format!("WALL_MS {n}"));
        }
        format!("FEDERATION BUDGET ({}) {body}", limits.join(", "))
    } else {
        body
    };
    Ok(match stmt.mode {
        Mode::Run => body,
        Mode::Explain => format!("EXPLAIN {body}"),
        Mode::Profile => format!("PROFILE {body}"),
    })
}

/// ` WITH PROOF, KNOWLEDGE (a, b)` — empty when the statement has no annotation.
fn annotations_to_uql(annotations: &Annotations) -> String {
    let mut parts: Vec<String> = Vec::new();
    if annotations.proof {
        parts.push("PROOF".into());
    }
    match annotations.knowledge.as_deref() {
        Some([]) => parts.push("KNOWLEDGE".into()),
        Some(columns) => {
            let columns: Vec<String> = columns.iter().map(|c| uql_ident(c)).collect();
            parts.push(format!("KNOWLEDGE ({})", columns.join(", ")));
        }
        None => {}
    }
    if parts.is_empty() {
        return String::new();
    }
    format!(" WITH {}", parts.join(", "))
}

/// A `LET … ; FROM/JOIN …` program for a DAG. Chains of single-input, single-consumer
/// nodes become one pipeline; a node whose output is shared or joined ends a named
/// binding (`b<first node id>`). The text is verified to re-parse to the identical DAG;
/// a DAG whose node order is not the parser's first-reference order is refused.
pub fn dag_to_uql(nodes: &[DagNode]) -> Result<String, UqlPrintError> {
    let dangling = nodes
        .iter()
        .enumerate()
        .any(|(id, n)| n.inputs.iter().any(|&i| i >= id));
    if dangling {
        return Err(UqlPrintError {
            code: UqlPrintCode::DegenerateShape,
            detail: "a DAG node consumes itself or a later node".into(),
        });
    }
    let layout = ChainLayout::of(nodes);
    let mut out = String::new();
    for &start in layout.starts.iter().filter(|&&s| s != layout.main) {
        out.push_str(&format!(
            "LET b{start} = {};\n",
            layout.chain_text(nodes, start)?
        ));
    }
    out.push_str(&layout.chain_text(nodes, layout.main)?);
    let reparsed = parse_statement(&out, &Params::new()).map(|s| s.body);
    let chain_plan = is_chain(nodes)
        .then(|| Body::Pipeline(Plan::new(nodes.iter().map(|n| n.op.clone()).collect())));
    let exact = reparsed.as_ref().ok() == Some(&Body::Dag(nodes.to_vec()));
    if !exact && reparsed.ok() != chain_plan {
        return Err(UqlPrintError {
            code: UqlPrintCode::DegenerateShape,
            detail: "the DAG's node order is not the first-reference order UQL produces".into(),
        });
    }
    Ok(out)
}

/// Is the DAG one linear chain (its canonical form is then a plain pipeline)?
fn is_chain(nodes: &[DagNode]) -> bool {
    nodes
        .iter()
        .enumerate()
        .all(|(id, n)| n.inputs == if id == 0 { vec![] } else { vec![id - 1] })
}

/// The chain decomposition of a DAG.
struct ChainLayout {
    /// First node of every chain, ascending.
    starts: Vec<usize>,
    /// Node → the first node of its chain.
    chain_of: BTreeMap<usize, usize>,
    /// Chain start → its nodes in order.
    members: BTreeMap<usize, Vec<usize>>,
    /// The chain holding the sink (the last node).
    main: usize,
}

impl ChainLayout {
    fn of(nodes: &[DagNode]) -> Self {
        let mut consumers = vec![0usize; nodes.len()];
        for node in nodes {
            for &i in &node.inputs {
                consumers[i] += 1;
            }
        }
        let mut chain_of = BTreeMap::new();
        let mut members: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for (id, node) in nodes.iter().enumerate() {
            let start = match node.inputs.as_slice() {
                [p] if *p < id && consumers[*p] == 1 => chain_of[p],
                _ => id,
            };
            chain_of.insert(id, start);
            members.entry(start).or_default().push(id);
        }
        let main = nodes.len().checked_sub(1).map_or(0, |sink| chain_of[&sink]);
        Self {
            starts: members.keys().copied().collect(),
            chain_of,
            members,
            main,
        }
    }

    fn chain_text(&self, nodes: &[DagNode], start: usize) -> Result<String, UqlPrintError> {
        let head = match nodes[start].inputs.as_slice() {
            [] => None,
            [p] => Some(format!("FROM b{}", self.chain_of[p])),
            many => Some(format!(
                "JOIN {}",
                many.iter()
                    .map(|p| format!("b{}", self.chain_of[p]))
                    .collect::<Vec<_>>()
                    .join(", ")
            )),
        };
        let mut parts: Vec<String> = head.into_iter().collect();
        for &id in &self.members[&start] {
            parts.push(uql_op(&nodes[id].op)?);
        }
        Ok(parts.join(" |> "))
    }
}
