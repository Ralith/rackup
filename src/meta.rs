#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub name: Box<[u8]>,
    pub data: Data,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Data {
    Regular {
        offset: u64,
    },
    Link {
        target: Box<[u8]>,
    },
    Directory {
        offset: u64,
        files: u64,
    }
}
