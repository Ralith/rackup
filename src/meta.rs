#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct File {
    pub name: Vec<u8>,
    pub meta: Data,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Data {
    Regular {
        offset: u64,
        length: u64,
    },
    Link {
        target: Vec<u8>,
    },
    Directory {
        offset: u64,
        files: u64,
    }
}
