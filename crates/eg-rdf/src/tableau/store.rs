//! The completion graph's node store, shared copy-on-write between search branches.
//!
//! Every open choice point keeps a snapshot of the graph it branched from. A snapshot
//! of a `Vec<Node>` costs the whole graph, and a `Vec<Rc<Node>>` still one pointer per
//! node — with thousands of open choice points over a graph of thousands of nodes that
//! is gigabytes (EH-355: 14.7 GB in 70 s). Nodes are therefore kept in fixed-size
//! chunks, each behind its own `Rc`: a snapshot costs one pointer per chunk, and a
//! branch copies only the chunks and nodes it changes.

use std::ops::Index;
use std::rc::Rc;

use super::Node;

const CHUNK: usize = 64;

#[derive(Clone, Default)]
pub(super) struct NodeStore {
    chunks: Vec<Rc<Vec<Rc<Node>>>>,
    len: usize,
}

impl NodeStore {
    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn push(&mut self, node: Node) {
        if self.len.is_multiple_of(CHUNK) {
            self.chunks.push(Rc::new(Vec::with_capacity(CHUNK)));
        }
        let last = self
            .chunks
            .last_mut()
            .expect("a chunk was just ensured for the new node");
        Rc::make_mut(last).push(Rc::new(node));
        self.len += 1;
    }

    /// The node for writing, copied out of any snapshot that still shares it.
    pub(super) fn get_mut(&mut self, i: usize) -> &mut Node {
        let chunk = Rc::make_mut(&mut self.chunks[i / CHUNK]);
        Rc::make_mut(&mut chunk[i % CHUNK])
    }
}

impl Index<usize> for NodeStore {
    type Output = Node;

    fn index(&self, i: usize) -> &Node {
        &self.chunks[i / CHUNK][i % CHUNK]
    }
}
