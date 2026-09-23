//! Modality clauses (UQL-04): spatial (`geo`), tensor (`tensor`), time-series / sensor
//! fusion (`timeseries`), complex-event patterns (`stream`) and probabilistic scoring
//! (`probabilistic`). Every spelling here is the exact inverse of the eg-types printer.

use eg_types::wire::Op;

use super::Parser;
use crate::uql::diag::UqlError;
#[cfg(feature = "tensor")]
use crate::uql::lexer::Tok;

#[cfg(feature = "stream")]
mod cep;

impl<'a> Parser<'a> {
    gated! { "geo",
        /// `SPATIAL SCAN id BBOX [a,b,c,d]` | `SPATIAL <constructive op>`.
        fn spatial(&mut self) -> Result<Op, UqlError> {
            if self.eat_kw("SCAN") {
                let layer = self.id("a spatial layer")?;
                self.expect_kw("BBOX")?;
                let at = self.cur_start();
                let v = self.number_list("the bounding box")?;
                let bbox: [f64; 4] = v
                    .try_into()
                    .map_err(|_| self.err_at(at, "a BBOX has exactly four numbers"))?;
                return Ok(Op::SpatialScan { layer, bbox });
            }
            Ok(Op::SpatialOp {
                kind: self.spatial_op_kind()?,
            })
        }
    }

    #[cfg(feature = "geo")]
    fn spatial_op_kind(&mut self) -> Result<eg_types::wire::SpatialOpKind, UqlError> {
        use eg_types::wire::SpatialOpKind as K;
        let word = self.name("a spatial operation")?.to_ascii_uppercase();
        Ok(match word.as_str() {
            "BUFFER" => K::Buffer {
                distance: self.number("a buffer distance")?,
            },
            "CONVEX_HULL" => K::ConvexHull,
            "SIMPLIFY" => K::Simplify {
                tolerance: self.number("a tolerance")?,
            },
            "CENTROID" => K::Centroid,
            "UNION" => K::Union {
                wkt: self.string("a WKT geometry")?,
            },
            "INTERSECTION" => K::Intersection {
                wkt: self.string("a WKT geometry")?,
            },
            "DIFFERENCE" => K::Difference {
                wkt: self.string("a WKT geometry")?,
            },
            _ => {
                return Err(self.err_at(
                    self.prev_start(),
                    "expected SCAN, BUFFER, CONVEX_HULL, SIMPLIFY, CENTROID, UNION, \
                     INTERSECTION or DIFFERENCE after SPATIAL",
                ))
            }
        })
    }

    gated! { "geo",
        /// `REPROJECT TO int [FROM int]`.
        fn reproject(&mut self) -> Result<Op, UqlError> {
            self.expect_kw("TO")?;
            let to_epsg = self.parse_number::<u32>("a target EPSG code")?;
            let from_epsg = if self.eat_kw("FROM") {
                Some(self.parse_number::<u32>("a source EPSG code")?)
            } else {
                None
            };
            Ok(Op::Reproject { to_epsg, from_epsg })
        }
    }

    gated! { "tensor",
        /// `TENSOR SCAN id` | `TENSOR SLICE […]` | `TENSOR REDUCE agg AXIS n` | `TENSOR op num`.
        fn tensor(&mut self) -> Result<Op, UqlError> {
            if self.eat_kw("SCAN") {
                return Ok(Op::TensorScan {
                    layer: self.id("a tensor layer")?,
                });
            }
            Ok(Op::TensorOp {
                kind: self.tensor_op_kind()?,
            })
        }
    }

    #[cfg(feature = "tensor")]
    fn tensor_op_kind(&mut self) -> Result<eg_types::wire::TensorOpKind, UqlError> {
        use eg_types::wire::{TensorElementwiseOp as E, TensorOpKind as K};
        let word = self.name("a tensor operation")?.to_ascii_uppercase();
        let op = match word.as_str() {
            "SLICE" => {
                return Ok(K::Slice {
                    ranges: self.bracket_list("the slice ranges", |p| p.slice_range())?,
                })
            }
            "REDUCE" => return self.tensor_reduce(),
            "ADD" => E::Add,
            "SUB" => E::Sub,
            "MUL" => E::Mul,
            "DIV" => E::Div,
            _ => {
                return Err(self.err_at(
                    self.prev_start(),
                    "expected SCAN, SLICE, REDUCE, ADD, SUB, MUL or DIV after TENSOR",
                ))
            }
        };
        Ok(K::Elementwise {
            op,
            scalar: self.number("a scalar operand")?,
        })
    }

    #[cfg(feature = "tensor")]
    fn tensor_reduce(&mut self) -> Result<eg_types::wire::TensorOpKind, UqlError> {
        use eg_types::wire::{TensorOpKind as K, TensorReduceKind as R};
        let kind = match self.name("a reduction")?.to_ascii_uppercase().as_str() {
            "SUM" => R::Sum,
            "MEAN" => R::Mean,
            "MAX" => R::Max,
            "MIN" => R::Min,
            _ => return Err(self.err_at(self.prev_start(), "expected SUM, MEAN, MAX or MIN")),
        };
        self.expect_kw("AXIS")?;
        Ok(K::Reduce {
            axis: self.parse_number::<usize>("an axis")?,
            kind,
        })
    }

    #[cfg(feature = "tensor")]
    fn slice_range(&mut self) -> Result<(usize, usize), UqlError> {
        let a = self.parse_number::<usize>("a slice start")?;
        self.expect(&Tok::Colon, "`:` in a slice range")?;
        Ok((a, self.parse_number::<usize>("a slice end")?))
    }

    gated! { "timeseries",
        /// `TSSCAN […] FROM num TO num`.
        fn tsscan(&mut self) -> Result<Op, UqlError> {
            let series = self.string_list("the series list")?;
            self.expect_kw("FROM")?;
            let from = self.number("a start time (seconds)")?;
            self.expect_kw("TO")?;
            Ok(Op::TsScan {
                series,
                from,
                to: self.number("an end time (seconds)")?,
            })
        }
    }

    gated! { "timeseries",
        /// `SENSOR FUSE […] TOLERANCE int` | `SENSOR ALIGN [...] CLOCK … [TOLERANCE int]`.
        fn sensor(&mut self) -> Result<Op, UqlError> {
            if self.eat_kw("FUSE") {
                let streams = self.string_list("the stream list")?;
                self.expect_kw("TOLERANCE")?;
                return Ok(Op::SensorFuse {
                    streams,
                    tolerance_ns: self.parse_number::<u64>("a tolerance (ns)")?,
                });
            }
            self.expect_kw("ALIGN")?;
            let streams = self.bracket_list("the stream list", |p| p.fuse_stream())?;
            self.expect_kw("CLOCK")?;
            let clock = self.fuse_clock()?;
            let tolerance_ns = if self.eat_kw("TOLERANCE") {
                Some(self.parse_number::<u64>("a tolerance (ns)")?)
            } else {
                None
            };
            Ok(Op::SensorAlign {
                streams,
                clock,
                tolerance_ns,
            })
        }
    }

    #[cfg(feature = "timeseries")]
    fn fuse_stream(&mut self) -> Result<eg_types::wire::FuseStream, UqlError> {
        use eg_types::wire::FuseInterp as I;
        let layer = self.string("a stream layer")?;
        let interp = match self.name("an interpolation")?.to_ascii_uppercase().as_str() {
            "NEAREST" => I::Nearest,
            "LINEAR" => I::Linear,
            "ASOF_HOLD" => I::AsofHold,
            _ => {
                return Err(self.err_at(self.prev_start(), "expected NEAREST, LINEAR or ASOF_HOLD"))
            }
        };
        Ok(eg_types::wire::FuseStream { layer, interp })
    }

    #[cfg(feature = "timeseries")]
    fn fuse_clock(&mut self) -> Result<eg_types::wire::FuseClock, UqlError> {
        use eg_types::wire::FuseClock as C;
        if self.eat_kw("TUMBLING") {
            self.expect_kw("WIDTH")?;
            let width_ns = self.parse_number::<i64>("a window width (ns)")?;
            self.expect_kw("STEP")?;
            return Ok(C::Tumbling {
                width_ns,
                step_ns: self.parse_number::<i64>("a step (ns)")?,
            });
        }
        self.expect_kw("UNIFORM")?;
        self.expect_kw("FROM")?;
        let from_ns = self.parse_number::<i64>("a start (ns)")?;
        self.expect_kw("TO")?;
        let to_ns = self.parse_number::<i64>("an end (ns)")?;
        self.expect_kw("STEP")?;
        Ok(C::Uniform {
            from_ns,
            to_ns,
            step_ns: self.parse_number::<i64>("a step (ns)")?,
        })
    }

    gated! { "stream",
        /// `CEP cep_node WINDOW (SLIDING | TUMBLING) int`.
        fn cep(&mut self) -> Result<Op, UqlError> {
            Ok(Op::Cep {
                pattern: self.cep_pattern()?,
            })
        }
    }

    gated! { "probabilistic",
        /// `PROB EXPECTATION | MARGINAL … | CONDITIONAL … | SAMPLE SEED int`.
        fn prob(&mut self) -> Result<Op, UqlError> {
            use eg_types::wire::ProbQuery as Q;
            let word = self.name("a probabilistic query")?.to_ascii_uppercase();
            let query = match word.as_str() {
                "EXPECTATION" => Q::Expectation,
                "MARGINAL" => self.prob_marginal()?,
                "CONDITIONAL" => Q::Conditional {
                    evidence: self.prob_evidence()?,
                },
                "SAMPLE" => {
                    self.expect_kw("SEED")?;
                    Q::Sample {
                        seed: self.parse_number::<u64>("a seed")?,
                    }
                }
                _ => {
                    return Err(self.err_at(
                        self.prev_start(),
                        "expected EXPECTATION, MARGINAL, CONDITIONAL or SAMPLE after PROB",
                    ))
                }
            };
            Ok(Op::Probabilistic { query })
        }
    }

    #[cfg(feature = "probabilistic")]
    fn prob_marginal(&mut self) -> Result<eg_types::wire::ProbQuery, UqlError> {
        let at = if self.eat_kw("AT") {
            self.number("a marginal point")?
        } else {
            0.0
        };
        let label = if self.eat_kw("LABEL") {
            Some(self.string("a label")?)
        } else {
            None
        };
        Ok(eg_types::wire::ProbQuery::Marginal { at, label })
    }

    #[cfg(feature = "probabilistic")]
    fn prob_evidence(&mut self) -> Result<eg_types::wire::ProbEvidenceSpec, UqlError> {
        use eg_types::wire::ProbEvidenceSpec as E;
        if self.eat_kw("BERNOULLI") {
            let successes = self.number("a success count")?;
            return Ok(E::Bernoulli {
                successes,
                failures: self.number("a failure count")?,
            });
        }
        self.expect_kw("GAUSSIAN")?;
        let observations = self.number_list("the observations")?;
        self.expect_kw("VARIANCE")?;
        Ok(E::Gaussian {
            observations,
            known_variance: self.number("the known variance")?,
        })
    }
}
