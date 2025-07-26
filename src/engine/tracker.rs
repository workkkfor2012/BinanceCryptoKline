// src/engine/tracker.rs
#[derive(Debug)]
pub struct DirtyTracker {
    updated_indices: Vec<usize>,
    is_dirty_flags: Vec<bool>,
}

impl DirtyTracker {
    pub fn new(capacity: usize) -> Self {
        Self {
            updated_indices: Vec::with_capacity(1024), // 初始容量
            is_dirty_flags: vec![false; capacity],
        }
    }

    pub fn mark_dirty(&mut self, kline_offset: usize) {
        if let Some(flag) = self.is_dirty_flags.get_mut(kline_offset) {
            if !*flag {
                *flag = true;
                self.updated_indices.push(kline_offset);
            }
        }
    }

    /// 收集所有脏K线的索引，并重置追踪器。
    pub fn collect_and_reset(&mut self) -> Vec<usize> {
        if self.updated_indices.is_empty() {
            return Vec::new();
        }
        let dirty = std::mem::take(&mut self.updated_indices);
        for &index in &dirty {
            if let Some(flag) = self.is_dirty_flags.get_mut(index) {
                *flag = false;
            }
        }
        dirty
    }

    pub fn is_empty(&self) -> bool {
        self.updated_indices.is_empty()
    }
    
    // 如果支持动态增加品种，可能需要resize
    pub fn resize(&mut self, new_capacity: usize) {
        self.is_dirty_flags.resize(new_capacity, false);
    }
}
