use std::collections::{HashMap, VecDeque};

use crate::storage::Document;

#[derive(Clone, Debug)]
pub struct CacheEntry {
    pub doc: Document,
    pub size_bytes: usize,
}

#[derive(Debug)]
pub struct DocCache {
    capacity_bytes: usize,
    size_bytes: usize,
    entries: HashMap<String, CacheEntry>,
    lru_order: VecDeque<String>,
}

impl DocCache {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            size_bytes: 0,
            entries: HashMap::new(),
            lru_order: VecDeque::new(),
        }
    }

    pub fn get(&mut self, id: &str) -> Option<Document> {
        if let Some(entry) = self.entries.get(id) {
            let doc_clone = entry.doc.clone();
            self.touch(id);
            return Some(doc_clone);
        }
        None
    }

    pub fn insert(&mut self, id: String, doc: Document) {
        let size_bytes = estimate_doc_size_bytes(&doc);
        if size_bytes > self.capacity_bytes {
            return;
        }
        if let Some(existing) = self.entries.remove(&id) {
            self.size_bytes = self.size_bytes.saturating_sub(existing.size_bytes);
            self.lru_order.retain(|key| key != &id);
        }
        while self.size_bytes + size_bytes > self.capacity_bytes {
            if let Some(evict_id) = self.lru_order.pop_back() {
                if let Some(evicted) = self.entries.remove(&evict_id) {
                    self.size_bytes = self.size_bytes.saturating_sub(evicted.size_bytes);
                }
            } else {
                break;
            }
        }
        self.size_bytes += size_bytes;
        self.lru_order.push_front(id.clone());
        self.entries.insert(id, CacheEntry { doc, size_bytes });
    }

    pub fn remove(&mut self, id: &str) {
        if let Some(entry) = self.entries.remove(id) {
            self.size_bytes = self.size_bytes.saturating_sub(entry.size_bytes);
            self.lru_order.retain(|key| key != id);
        }
    }

    fn touch(&mut self, id: &str) {
        self.lru_order.retain(|key| key != id);
        self.lru_order.push_front(id.to_string());
    }
}

fn estimate_doc_size_bytes(doc: &Document) -> usize {
    doc.id.len()
        + doc.text.len()
        + doc.category.len()
        + doc.vector.len() * std::mem::size_of::<f32>()
        + doc.metadata.to_string().len()
}
