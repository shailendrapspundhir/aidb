use std::collections::{HashMap, VecDeque};
use tracing::{debug, trace, instrument};

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
    #[instrument(skip(capacity_bytes))]
    pub fn new(capacity_bytes: usize) -> Self {
        debug!(capacity_bytes = capacity_bytes, "Creating document cache");
        Self {
            capacity_bytes,
            size_bytes: 0,
            entries: HashMap::new(),
            lru_order: VecDeque::new(),
        }
    }

    #[instrument(skip(self))]
    pub fn get(&mut self, id: &str) -> Option<Document> {
        if let Some(entry) = self.entries.get(id) {
            let doc_clone = entry.doc.clone();
            let size_bytes = entry.size_bytes;
            self.touch(id);
            trace!(id = %id, size_bytes = size_bytes, "Cache hit");
            return Some(doc_clone);
        }
        trace!(id = %id, "Cache miss");
        None
    }

    #[instrument(skip(self, doc))]
    pub fn insert(&mut self, id: String, doc: Document) {
        let size_bytes = estimate_doc_size_bytes(&doc);
        
        if size_bytes > self.capacity_bytes {
            debug!(
                id = %id, 
                doc_size_bytes = size_bytes, 
                capacity_bytes = self.capacity_bytes,
                "Document too large for cache, skipping"
            );
            return;
        }
        
        if let Some(existing) = self.entries.remove(&id) {
            self.size_bytes = self.size_bytes.saturating_sub(existing.size_bytes);
            self.lru_order.retain(|key| key != &id);
            trace!(id = %id, "Updating existing cache entry");
        }
        
        // Evict entries if necessary
        let mut evicted_count = 0;
        while self.size_bytes + size_bytes > self.capacity_bytes {
            if let Some(evict_id) = self.lru_order.pop_back() {
                if let Some(evicted) = self.entries.remove(&evict_id) {
                    self.size_bytes = self.size_bytes.saturating_sub(evicted.size_bytes);
                    evicted_count += 1;
                }
            } else {
                break;
            }
        }
        
        if evicted_count > 0 {
            trace!(evicted_count = evicted_count, "Evicted cache entries to make room");
        }
        
        self.size_bytes += size_bytes;
        self.lru_order.push_front(id.clone());
        self.entries.insert(id, CacheEntry { doc, size_bytes });
        
        trace!(
            entries = self.entries.len(),
            current_size_bytes = self.size_bytes,
            capacity_bytes = self.capacity_bytes,
            "Cache entry inserted"
        );
    }

    #[instrument(skip(self))]
    pub fn remove(&mut self, id: &str) {
        if let Some(entry) = self.entries.remove(id) {
            self.size_bytes = self.size_bytes.saturating_sub(entry.size_bytes);
            self.lru_order.retain(|key| key != id);
            trace!(id = %id, size_freed_bytes = entry.size_bytes, "Cache entry removed");
        }
    }

    #[instrument(skip(self))]
    fn touch(&mut self, id: &str) {
        self.lru_order.retain(|key| key != id);
        self.lru_order.push_front(id.to_string());
        trace!(id = %id, "Cache entry touched (moved to front of LRU)");
    }
}

fn estimate_doc_size_bytes(doc: &Document) -> usize {
    doc.id.len()
        + doc.text.len()
        + doc.category.len()
        + doc.vector.len() * std::mem::size_of::<f32>()
        + doc.metadata.to_string().len()
}
