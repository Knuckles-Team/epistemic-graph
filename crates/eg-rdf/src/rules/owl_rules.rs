//! OWL 2 RL ABox rules derived from a parsed ontology.

use super::syntax::{iri, OWL_SAME_AS};
use super::{Atom, RTerm, Rule};
use crate::owl::Ontology;

// ── Built-in OWL 2 RL rules derived from a parsed Ontology ────────────────────

/// Build the instance-level (ABox) OWL 2 RL rules implied by a parsed [`Ontology`]:
/// subClassOf, subPropertyOf, domain, range, symmetric, inverse, property-chains /
/// transitive, and functional / inverse-functional → `owl:sameAs`. These run in the
/// SAME fixpoint as the user's custom rules, so a custom rule can build on (and feed)
/// OWL-inferred facts (CONCEPT:EG-KG.ontology.eg-runtime-swrl-datalog).
pub fn builtin_rules(ont: &Ontology) -> Vec<Rule> {
    let mut rules: Vec<Rule> = Vec::new();
    let mut n = 0usize;
    push_subclass_rules(ont, &mut rules, &mut n);
    push_subprop_rules(ont, &mut rules, &mut n);
    push_domain_range_rules(ont, &mut rules, &mut n);
    push_symmetric_rules(ont, &mut rules, &mut n);
    push_inverse_rules(ont, &mut rules, &mut n);
    push_chain_rules(ont, &mut rules, &mut n);
    push_functional_rules(ont, &mut rules, &mut n);
    rules
}

/// Allocate the next deterministic builtin-rule name (`builtin:<prefix>:<n>`), shared
/// across all the `push_*_rules` helpers below so numbering stays contiguous exactly as
/// it was when `builtin_rules` built every rule inline.
fn named(prefix: &str, n: &mut usize) -> String {
    *n += 1;
    format!("builtin:{prefix}:{n}")
}

/// Shorthand for the three rule variables shared by every builtin-rule helper below.
fn var(name: &str) -> RTerm {
    RTerm::Var(name.into())
}

/// Named subClassOf (single + conjunctive LHS, named RHS): B(x) [^ …] -> C(x).
fn push_subclass_rules(ont: &Ontology, rules: &mut Vec<Rule>, n: &mut usize) {
    use crate::owl::Concept;
    for g in &ont.gcis {
        let rhs = match &g.rhs {
            Concept::Named(c) => c.clone(),
            Concept::Some(..) => continue, // existential ⇒ TBox EL completion, not ABox
        };
        let mut body = Vec::new();
        let mut all_named = true;
        for c in &g.lhs {
            match c {
                Concept::Named(b) => body.push(Atom {
                    pred: b.clone(),
                    args: vec![var("x")],
                }),
                Concept::Some(..) => {
                    all_named = false;
                    break;
                }
            }
        }
        if all_named && !body.is_empty() {
            rules.push(Rule {
                name: named("subclass", n),
                body,
                head: vec![Atom {
                    pred: rhs,
                    args: vec![var("x")],
                }],
                conf: g.conf,
            });
        }
    }
}

/// subPropertyOf (incl. equivalentProperty's two directions): r(x,y) -> s(x,y).
fn push_subprop_rules(ont: &Ontology, rules: &mut Vec<Rule>, n: &mut usize) {
    for (r, s, _label, conf) in &ont.sub_roles {
        rules.push(Rule {
            name: named("subprop", n),
            body: vec![Atom {
                pred: r.clone(),
                args: vec![var("x"), var("y")],
            }],
            head: vec![Atom {
                pred: s.clone(),
                args: vec![var("x"), var("y")],
            }],
            conf: *conf,
        });
    }
}

/// domain(r,D): r(x,y) -> D(x).  range(r,D): r(x,y) -> D(y).
fn push_domain_range_rules(ont: &Ontology, rules: &mut Vec<Rule>, n: &mut usize) {
    for (r, d) in &ont.domains {
        rules.push(Rule {
            name: named("domain", n),
            body: vec![Atom {
                pred: r.clone(),
                args: vec![var("x"), var("y")],
            }],
            head: vec![Atom {
                pred: d.clone(),
                args: vec![var("x")],
            }],
            conf: 1.0,
        });
    }
    for (r, d) in &ont.ranges {
        rules.push(Rule {
            name: named("range", n),
            body: vec![Atom {
                pred: r.clone(),
                args: vec![var("x"), var("y")],
            }],
            head: vec![Atom {
                pred: d.clone(),
                args: vec![var("y")],
            }],
            conf: 1.0,
        });
    }
}

/// symmetric r: r(x,y) -> r(y,x).
fn push_symmetric_rules(ont: &Ontology, rules: &mut Vec<Rule>, n: &mut usize) {
    for r in &ont.symmetric {
        rules.push(Rule {
            name: named("symmetric", n),
            body: vec![Atom {
                pred: r.clone(),
                args: vec![var("x"), var("y")],
            }],
            head: vec![Atom {
                pred: r.clone(),
                args: vec![var("y"), var("x")],
            }],
            conf: 1.0,
        });
    }
}

/// inverse (p1,p2): p1(x,y) -> p2(y,x) and p2(x,y) -> p1(y,x).
fn push_inverse_rules(ont: &Ontology, rules: &mut Vec<Rule>, n: &mut usize) {
    for (p1, p2) in &ont.inverses {
        rules.push(Rule {
            name: named("inverse", n),
            body: vec![Atom {
                pred: p1.clone(),
                args: vec![var("x"), var("y")],
            }],
            head: vec![Atom {
                pred: p2.clone(),
                args: vec![var("y"), var("x")],
            }],
            conf: 1.0,
        });
        rules.push(Rule {
            name: named("inverse", n),
            body: vec![Atom {
                pred: p2.clone(),
                args: vec![var("x"), var("y")],
            }],
            head: vec![Atom {
                pred: p1.clone(),
                args: vec![var("y"), var("x")],
            }],
            conf: 1.0,
        });
    }
}

/// property chains (covers transitive r∘r⊑r): r1(x,y) ^ r2(y,z) -> s(x,z).
fn push_chain_rules(ont: &Ontology, rules: &mut Vec<Rule>, n: &mut usize) {
    for ch in &ont.chains {
        if ch.chain.len() == 2 {
            rules.push(Rule {
                name: named("chain", n),
                body: vec![
                    Atom {
                        pred: ch.chain[0].clone(),
                        args: vec![var("x"), var("y")],
                    },
                    Atom {
                        pred: ch.chain[1].clone(),
                        args: vec![var("y"), var("z")],
                    },
                ],
                head: vec![Atom {
                    pred: ch.sup.clone(),
                    args: vec![var("x"), var("z")],
                }],
                conf: ch.conf,
            });
        }
    }
}

/// FunctionalProperty r: r(x,y) ^ r(x,z) -> sameAs(y,z).
/// InverseFunctionalProperty r: r(y,x) ^ r(z,x) -> sameAs(y,z).
fn push_functional_rules(ont: &Ontology, rules: &mut Vec<Rule>, n: &mut usize) {
    for r in &ont.functional {
        rules.push(Rule {
            name: named("functional", n),
            body: vec![
                Atom {
                    pred: r.clone(),
                    args: vec![var("x"), var("y")],
                },
                Atom {
                    pred: r.clone(),
                    args: vec![var("x"), var("z")],
                },
            ],
            head: vec![Atom {
                pred: iri(OWL_SAME_AS),
                args: vec![var("y"), var("z")],
            }],
            conf: 1.0,
        });
    }
    for r in &ont.inverse_functional {
        rules.push(Rule {
            name: named("inverse-functional", n),
            body: vec![
                Atom {
                    pred: r.clone(),
                    args: vec![var("y"), var("x")],
                },
                Atom {
                    pred: r.clone(),
                    args: vec![var("z"), var("x")],
                },
            ],
            head: vec![Atom {
                pred: iri(OWL_SAME_AS),
                args: vec![var("y"), var("z")],
            }],
            conf: 1.0,
        });
    }
}
